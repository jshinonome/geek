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
	"net"
	"os"
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
	ErrAuth                 = errors.New("geek: wrong credential?")
	ErrVersion              = errors.New("geek: require version 3+ kdb+")
	ErrNotConnected         = errors.New("geek: not connected")
	ErrMaxRetryTimesReached = errors.New("geek: maximum retry times reached")
	ErrNotAllowedAPI        = errors.New("geek: not allowed API")
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

func (q *QProcess) Async(args interface{}) error {
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

func (q *QProcess) Discard() error {
	msgLength, err := q.peekMsgLength()
	if err != nil {
		return err
	}
	_, err = q.reader.Discard(int(msgLength))
	return err
}

func (q *QProcess) Err(err error) error {
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
	peekSize := 1024
	msgLength, err := q.peekMsgLength()
	if err != nil {
		if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
			q.Close()
		}
		return "", err
	}
	if peekSize > int(msgLength) {
		peekSize = int(msgLength)
	}
	api, _ := q.reader.Peek(peekSize)
	var startIndex int
	if peekSize > 9 && api[8] == 245 {
		// a symbol
		startIndex = 9
	} else if peekSize > 14 && api[8] == 11 {
		// symbol list
		startIndex = 14
	} else if peekSize > 15 && api[14] == 245 {
		// mixed list
		startIndex = 15
	} else {
		return "", &GeekErr{"geek:PeekAPI not a symbol"}
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
