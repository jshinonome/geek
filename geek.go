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
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"syscall"
	"time"
)

type HandlerFunc func(*QProcess)

type QProcess struct {
	Port      int
	User      string
	Password  string
	Timeout   time.Duration
	TLSConfig *tls.Config
	conn      net.Conn
	writer    *bufio.Writer
	reader    *bufio.Reader
}

var (
	ErrAuth         = errors.New("geek: wrong credential?")
	ErrVersion      = errors.New("geek: require version 3+ kdb+")
	ErrNotConnected = errors.New("geek: not connected")
)

type GeekErr struct {
	Msg string
}

func (ge GeekErr) Error() string {
	return ge.Msg
}

func (q *QProcess) Close() {
	if q.conn != nil {
		q.conn.Close()
		q.conn = nil
	}
}

func (q QProcess) IsConnected() bool {
	return q.conn != nil
}

func (q *QProcess) Sync(k interface{}, args interface{}) error {
	if q.IsConnected() {
		err := writeIPC(q.writer, SYNC, args)
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				q.Close()
			}
			return err
		}
		err = readIPC(q.reader, k)
		if err != nil {
			// close when connection reset by peer or EOF error
			if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
				q.Close()
			}
			return err
		}
		return nil
	} else {
		return ErrNotConnected
	}
}

func (q QProcess) Async(args interface{}) error {
	if q.IsConnected() {
		err := writeIPC(q.writer, ASYNC, args)
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				q.Close()
			}
			return err
		}
		return nil
	} else {
		return ErrNotConnected
	}
}

func (q QProcess) Err(err error) error {
	if q.IsConnected() {
		err := writeErr(q.writer, err)
		if err != nil {
			if errors.Is(err, syscall.EPIPE) {
				q.Close()
			}
			return err
		}
		return nil
	} else {
		return ErrNotConnected
	}
}

func (q *QProcess) auth() error {
	buf := bytes.NewBufferString(q.User + ":" + q.Password)
	// test with v3 capability (compression, timestamp, timespan, uuid)
	buf.WriteByte(3)
	buf.WriteByte(0)
	_, err := q.conn.Write(buf.Bytes())
	if err != nil {
		q.conn.Close()
		return err
	}
	ipcVersion := make([]byte, 1)
	_, err = q.conn.Read(ipcVersion)
	if err != nil {
		q.conn.Close()
		return ErrAuth
	}
	if ipcVersion[0] < 3 {
		return ErrVersion
	}
	return nil
}

func (q *QProcess) Dial() error {
	socket := fmt.Sprintf(":%d", q.Port)
	var conn net.Conn
	var err error
	if q.TLSConfig != nil {
		conn, err = tls.Dial("tcp", socket, q.TLSConfig)
	} else {
		conn, err = net.DialTimeout("tcp", socket, q.Timeout)
	}
	if err != nil {
		return err
	}
	q.conn = conn
	err = q.auth()
	if err != nil {
		q.conn = nil
		return err
	}
	conn.(*net.TCPConn).SetKeepAlive(true)
	q.reader = bufio.NewReader(conn)
	q.writer = bufio.NewWriter(conn)
	return nil
}

type QConnPool struct {
	c              chan int
	Conns          map[int]*QProcess
	Timeout        time.Duration
	ReviveInterval time.Duration
	DeadConns      map[int]bool
	RetryTimes     int
}

func (pool *QConnPool) Put(q *QProcess) {
	if pool.Conns == nil {
		pool.Conns = make(map[int]*QProcess)
		pool.DeadConns = make(map[int]bool)
	}
	pool.Conns[q.Port] = q
	pool.DeadConns[q.Port] = false
}

func (pool *QConnPool) Serving() error {
	if pool.Conns == nil {
		return &GeekErr{"geek:Serving Empty connection pool"}
	}
	if pool.Timeout <= 0 {
		log.Println("Timeout <= 0, set it to 60s")
		pool.Timeout = time.Minute
	}
	if pool.RetryTimes <= 0 {
		log.Println("Retry Times >= 0, set it to 3")
		if len(pool.Conns) >= 3 {
			pool.RetryTimes = 3
		} else {
			pool.RetryTimes = len(pool.Conns)
		}
	}
	if pool.ReviveInterval <= 0 {
		pool.ReviveInterval = 15 * time.Second
	}
	pool.c = make(chan int, len(pool.Conns))
	for k := range pool.Conns {
		pool.c <- k
	}
	go pool.Revive()
	return nil
}

func (pool *QConnPool) Sync(k interface{}, args interface{}) error {
	for i := 0; i < pool.RetryTimes; i++ {
		port := <-pool.c
		q := pool.Conns[port]
		err := q.Sync(k, args)
		if err != nil {
			// retry if q is not connected, retry
			if !q.IsConnected() {
				pool.DeadConns[q.Port] = true
				continue
			}
			pool.c <- port
			return err
		}
		return nil
	}
	return &GeekErr{"Maximum retry times reached"}
}

func (pool *QConnPool) Revive() {
	for {
		for k, v := range pool.DeadConns {
			if v {
				log.Printf("Try to revive %d", k)
				q := pool.Conns[k]
				err := q.Dial()
				if err != nil {
					log.Printf("Failed to revive %d with error:%s", k, err)
					continue
				}
				pool.DeadConns[k] = false
				pool.c <- k
			}
		}
		time.Sleep(pool.ReviveInterval)
	}
}

func (pool *QConnPool) Handle(qClient QProcess) error {
	for i := 0; i < pool.RetryTimes; i++ {
		port := <-pool.c
		q := pool.Conns[port]
		deadline := time.Now().Add(pool.Timeout)
		qClient.conn.SetDeadline(deadline)
		n, err := qClient.peekMsgLength()
		if err != nil {
			log.Printf("geek:Handle failed to handle %s@%s", qClient.User, qClient.conn.RemoteAddr())
			qClient.Close()
			pool.c <- port
			return err
		}
		_, err = io.CopyN(q.writer, qClient.reader, n)
		if err != nil {
			log.Println(err)
			// read errors
			if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
				qClient.Close()
				pool.c <- port
				return err
			} else {
				pool.DeadConns[port] = true
				continue
			}
		}
		q.conn.SetDeadline(deadline)
		n, err = q.peekMsgLength()
		if err != nil {
			if !q.IsConnected() {
				pool.DeadConns[port] = true
				continue
			} else {
				pool.c <- port
				continue
			}
		}
		_, err = io.CopyN(qClient.writer, q.reader, n)
		if err != nil {
			log.Println(err)
			if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
				qClient.Close()
				pool.c <- port
				return err
			} else {
				pool.DeadConns[port] = true
				continue
			}
		}
		qClient.Close()
		pool.c <- q.Port
		return nil
	}
	errMsg := &GeekErr{"Maximum retry times reached"}
	qClient.Err(errMsg)
	qClient.Close()
	return errMsg
}

type QEngine struct {
	Port int
	Auth func(string, string) error
	Pool *QConnPool
}

func (q *QEngine) Run() error {
	socket := fmt.Sprintf(":%d", q.Port)
	listener, err := net.Listen("tcp", socket)
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("QEngine:Run Failed to accept connection %s", err)
			continue
		}
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		credentials, _ := reader.ReadBytes(0x00)
		var colonPos int
		for i, c := range credentials {
			if c == ':' {
				colonPos = i
				break
			}
		}
		var user, password string
		if colonPos == 0 {
			user = string(credentials[:len(credentials)-2])
		} else {
			user = string(credentials[:colonPos-1])
			password = string(credentials[colonPos+1 : len(credentials)-2])
		}
		err = q.Auth(user, password)
		if err != nil {
			log.Printf("QEngine:Run Authentication failed %s", err)
			conn.Close()
		} else {
			conn.Write([]byte{0x03})
			q.Pool.Handle(QProcess{conn: conn, reader: reader, writer: writer, User: user})
		}
	}
}

// first arg from a kdb client must be api name
func (q *QProcess) PeekAPI() (string, error) {
	_, err := q.reader.Peek(1)
	if err != nil {
		if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
			q.Close()
		}
		return "", err
	}
	peekSize := 1024
	if peekSize > q.reader.Buffered() {
		peekSize = q.reader.Buffered()
	}
	api, _ := q.reader.Peek(peekSize)
	var startIndex int
	if api[14] == 245 {
		// mixed list
		startIndex = 15
	} else if api[8] == 11 {
		// symbol list
		startIndex = 14
	} else {
		return "", &GeekErr{"geek:PeekAPI first arg is not a symbol"}
	}
	var endIndex int
	for i := startIndex; i < len(api); i++ {
		if api[i] == 0 {
			endIndex = i
			break
		}
	}
	if endIndex == 0 {
		return "", &GeekErr{"geek:PeekAPI api name >= 1009 chars"}
	} else if endIndex == startIndex {
		return "", &GeekErr{"geek:PeekAPI null api name"}
	}
	return string(api[startIndex:endIndex]), nil
}

// int64 is for io.copyN
func (q *QProcess) peekMsgLength() (int64, error) {
	header, err := q.reader.Peek(8)
	if err != nil {
		if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
			q.Close()
		}
		return 0, err
	}
	return int64(binary.LittleEndian.Uint32(header[4:8])), nil
}
