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
	"os"
	"sync"
	"syscall"
	"time"
)

var DefaultLogWriter io.Writer = os.Stdout

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

// var names used on the q processes
const (
	GeekUser string = ".geek.user"
	GeekIP   string = ".geek.ip"
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

type ConnPool struct {
	c              chan int
	mu             sync.Mutex
	Conns          map[int]*QProcess
	Timeout        time.Duration
	ReviveInterval time.Duration
	DeadConns      map[int]bool
	RetryTimes     int
	queueCounter   int
}

func NewConnPool() *ConnPool {
	return &ConnPool{
		Timeout:        time.Minute,
		RetryTimes:     3,
		ReviveInterval: 15 * time.Second,
	}
}

func (pool *ConnPool) Put(q *QProcess) {
	if pool.Conns == nil {
		pool.Conns = make(map[int]*QProcess)
		pool.DeadConns = make(map[int]bool)
	}
	pool.Conns[q.Port] = q
	pool.DeadConns[q.Port] = false
}

func (pool *ConnPool) increase() {
	pool.mu.Lock()
	pool.queueCounter++
	pool.mu.Unlock()
}

func (pool *ConnPool) decrease() {
	pool.mu.Lock()
	pool.queueCounter--
	pool.mu.Unlock()
}

func (pool *ConnPool) GetQueueCounter() int {
	return pool.queueCounter
}

func (pool *ConnPool) Serving() error {
	if pool.Conns == nil {
		return &GeekErr{"geek:Serving Empty connection pool"}
	}
	pool.c = make(chan int, len(pool.Conns))
	for k := range pool.Conns {
		pool.c <- k
	}
	go pool.Revive()
	return nil
}

func (pool *ConnPool) Sync(k interface{}, args interface{}) error {
	pool.increase()
	defer pool.decrease()

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
		pool.c <- port
		return nil
	}
	return &GeekErr{"Maximum retry times reached"}
}

func (pool *ConnPool) Revive() {
	for {
		for k, v := range pool.DeadConns {
			if v {
				log.Printf("[GEEK] Try to revive %d", k)
				q := pool.Conns[k]
				q.Close()
				err := q.Dial()
				if err != nil {
					log.Printf("[GEEK] Failed to revive %d with error:%s", k, err)
					continue
				}
				pool.DeadConns[k] = false
				pool.c <- k
			}
		}
		time.Sleep(pool.ReviveInterval)
	}
}

func (pool *ConnPool) Handle(qClient *QProcess) (int64, int64, error) {
	pool.increase()
	defer pool.decrease()

	deadline := time.Now().Add(pool.Timeout)
	qClient.conn.SetDeadline(deadline)
	inputSize, err := qClient.peekMsgLength()
	if err != nil {
		qClient.Close()
		return 0, 0, err
	}
	input := make([]byte, inputSize)
	// cached input for retrying
	qClient.reader.Read(input)
	// unset deadline
	qClient.conn.SetDeadline(time.Time{})
	defer qClient.Close()

	for i := 0; i < pool.RetryTimes; i++ {
		port := <-pool.c
		q := pool.Conns[port]

		setUser := struct {
			F    string
			Var  string
			User string
		}{".q.set", GeekUser, qClient.User}

		setIP := struct {
			F   string
			Var string
			IP  string
		}{".q.set", GeekIP, qClient.conn.RemoteAddr().String()}

		err := q.Async(setUser)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				pool.c <- port
			}
			continue
		}
		// unlikely to generate error
		q.Async(setIP)

		_, err = q.writer.Write(input)
		if err != nil {
			pool.DeadConns[port] = true
			continue
		}
		q.writer.Flush()
		// set timeout
		q.conn.SetDeadline(deadline)
		// unset timeout
		defer q.conn.SetDeadline(time.Time{})
		outputSize, err := q.peekMsgLength()
		if err != nil {
			if !q.IsConnected() {
				pool.DeadConns[port] = true
				continue
			}
			if errors.Is(err, os.ErrDeadlineExceeded) {
				qClient.Err(err)
			}
			pool.c <- port
			return inputSize, 0, err
		}
		_, err = io.CopyN(qClient.writer, q.reader, outputSize)
		if err != nil {
			// read error
			if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
				pool.DeadConns[port] = true
				continue
			} else {
				pool.c <- port
				return inputSize, outputSize, err
			}
		}
		pool.c <- q.Port
		return inputSize, outputSize, nil
	}

	errMsg := &GeekErr{"Maximum retry times reached"}
	qClient.Err(errMsg)
	return inputSize, 0, errMsg
}

type Authenticator func(string, string) error
type HandlerFunc func(*ConnPool, *QProcess)

type Engine struct {
	Port    int
	Auth    Authenticator
	Pool    *ConnPool
	Handler HandlerFunc
}

func DefaultEngine(pool *ConnPool) *Engine {
	noAuth := func(user string, password string) error {
		return nil
	}
	return &Engine{
		Port:    8101,
		Auth:    noAuth,
		Pool:    pool,
		Handler: LoggerWithConfig(LoggerConfig{}),
	}
}

func (e *Engine) Run() error {
	if e.Pool == nil {
		return &GeekErr{"QEngine:Run nil Pool"}
	}

	if e.Handler == nil {
		e.Handler = LoggerWithConfig(LoggerConfig{})
	}

	socket := fmt.Sprintf(":%d", e.Port)
	listener, err := net.Listen("tcp", socket)
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return &GeekErr{fmt.Sprintf("QEngine:Run Failed to accept connection %s", err)}
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
		err = e.Auth(user, password)
		if err != nil {
			conn.Close()
		} else {
			conn.Write([]byte{0x03})
			qClient := &QProcess{conn: conn, reader: reader, writer: writer, User: user}
			e.Handler(e.Pool, qClient)
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
