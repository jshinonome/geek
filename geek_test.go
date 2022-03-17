/**
 * Copyright 2022 Jo Shinonome
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package geek

import (
	"log"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var qClientPort = 9999
var qProcessPort = 9998
var qEnginePort = 9997

func TestMain(m *testing.M) {
	log.Println("Starting q processes")
	var qClient = exec.Command("q", "-p", strconv.Itoa(qClientPort), "-q")
	var qProcess = exec.Command("q", "-p", strconv.Itoa(qProcessPort), "-q")
	qClient.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}
	qProcess.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}
	stdin, err := qProcess.StdinPipe()
	if err != nil {
		log.Fatal("Failed to connect to stdin:", err)
	}
	stdout, err := qProcess.StdoutPipe()
	if err != nil {
		log.Fatal("Failed to connect to stdout:", err)
	}
	stderr, err := qProcess.StderrPipe()
	if err != nil {
		log.Fatal("Failed to connect to stderr:", err)
	}
	err = qProcess.Start()
	if err != nil {
		log.Fatal("Failed to start q client:", err)
	}
	err = qClient.Start()
	if err != nil {
		log.Fatal("Failed to start q server:", err)
	}
	go func() {
		buf := make([]byte, 512)
		for {
			n, err := stderr.Read(buf)
			log.Printf("q stderr: %s", string(buf[:n]))
			if err != nil {
				log.Printf("Failed to read q stderr: %s", err)
				return
			}
		}
	}()
	go func() {
		buf := make([]byte, 512)
		for {
			n, err := stdout.Read(buf)
			log.Printf("q stdout: %s", string(buf[:n]))
			if err != nil {
				log.Printf("Failed to read q stdout: %s", err)
				return
			}
		}
	}()
	// confirm process started
	stdin.Write([]byte("0\n"))
	stdout.Read([]byte{0, 0})

	// change user password for test authentication
	stdin.Write([]byte(".z.pw:{and[x~`test;y~\"test\"]}\n"))

	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestDial(t *testing.T) {
	q := QProcess{Port: qProcessPort, User: "test", Password: "test"}
	err := q.Dial()
	if err != nil {
		t.Errorf("Fail to connect to q process: %s", err)
	}
	q.Close()
	q = QProcess{Port: qProcessPort, User: "test", Password: "wrong"}
	err = q.Dial()
	if err == nil {
		t.Errorf("Should fail because of wrong credential")
	}
}

func TestServer(t *testing.T) {
	// hdb, rdb, rte etc.
	qProcess := QProcess{Port: qProcessPort, User: "test", Password: "test"}
	err := qProcess.Dial()
	if err != nil {
		t.Errorf("Failed to connect q process %v", err)
		return
	}
	pool := ConnPool{
		RetryTimes:     2,
		ReviveInterval: 2 * time.Second,
		Timeout:        time.Minute,
	}
	pool.Put(&qProcess)
	err = pool.Serving()
	if err != nil {
		t.Error(err)
		return
	}
	qEngine := Engine{
		Port: qEnginePort,
		Auth: func(u, p string) error { return nil },
		Pool: &pool,
	}
	qClient := QProcess{Port: qClientPort, User: "", Password: ""}
	err = qClient.Dial()
	if err != nil {
		t.Errorf("Failed to connect q client %v", err)
		return
	}
	done := make(chan bool)
	defer close(done)
	go func() {
		log.Println("Start q Engine")
		go qEngine.Run()
		done <- true
	}()
	time.AfterFunc(1*time.Second, func() {
		log.Printf("Query via q engine @%d", qEnginePort)
		var k string
		err := qClient.Sync(&k, []byte("`::9997 (`.Q.dd;`7203;`T)"))
		if err != nil {
			t.Error(err)
			done <- true
			return
		}
		if diff := cmp.Diff("7203.T", k); diff != "" {
			t.Error(diff)
		}
		var geekUser string
		qProcess.Sync(&geekUser, GeekUser)
		osUser, _ := user.Current()
		if diff := cmp.Diff(geekUser, osUser.Username); diff != "" {
			t.Error("User is not matached")
			t.Error(diff)
		}
		done <- true
	})
	// wait for goroutine complete
	<-done
	<-done
	// sync will fail, q process -> dead conns -> revive
	qProcess.Close()
	var k string
	err = pool.Sync(&k, []string{".Q.dd", "9984", "T"})
	if err != nil {
		t.Error(err)
	}
	if diff := cmp.Diff("9984.T", k); diff != "" {
		t.Error(diff)
	}

	pool.AllowedAPI = make(map[string]bool)
	err = qClient.Sync(&k, []byte("`::9997 (`.Q.dd;`2229;`T)"))
	if err.Error() != "`"+ErrNotAllowedAPI.Error() {
		t.Error(err)
	}
	pool.AllowedAPI[".Q.dd"] = true
	err = qClient.Sync(&k, []byte("`::9997 (`.Q.dd;`2229;`T)"))
	if err != nil {
		t.Error(err)
	}
	if diff := cmp.Diff("2229.T", k); diff != "" {
		t.Error(diff)
	}

	pool.RetryTimes = 1
	qProcess.Close()
	err = pool.Sync(&k, []string{".Q.dd", "9984", "T"})
	if err != ErrMaxRetryTimesReached {
		t.Error(err)
	}
	qProcess.Dial()
	var h1, h2 int64
	pool.reservedConns[qProcessPort].Sync(&h1, []byte("{`long$.z.w}()"))
	pool.Conns[qProcessPort].Sync(&h2, []byte("{`long$.z.w}()"))
	if h1 == 0 && h2 == 0 && h1 == h2 {
		t.Error("Should be different handles")
	}
	pool.Reload()
	qClient.Close()
}

func TestIPC(t *testing.T) {
	q := QProcess{Port: qProcessPort, User: "test", Password: "test"}
	err := q.Dial()
	if err != nil {
		t.Errorf("Fail to connect to q process: %s", err)
	}
	args := struct {
		F  []byte
		P1 string
		P2 int64
	}{
		[]byte("set"), "a", 18,
	}
	err = q.Async(args)
	if err != nil {
		t.Errorf("Fail to send async msg to q process: %s", err)
	}
	var a int64
	err = q.Sync(&a, "a")
	if err != nil {
		t.Errorf("Fail to send sync msg to q process: %s", err)
	}
	if a != 18 {
		t.Errorf("a should equal to 18")
	}
	q.Close()
}

func TestDisconnect(t *testing.T) {
	q := QProcess{Port: qClientPort, User: "test", Password: "test"}
	err := q.Dial()
	if err != nil {
		t.Errorf("Fail to connect to q client: %s", err)
	}
	q.Async([]byte("\\\\"))

	var time time.Time
	err = q.Sync(&time, []byte(".z.P"))
	// should be syscall.ECONNRESET error(connection reset by peer)
	if err == nil {
		t.Errorf("Should fail to send sync msg")
	}
	if q.IsConnected() {
		t.Errorf("Should be disconnected to qClient")
	}
}
