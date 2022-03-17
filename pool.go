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
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"syscall"
	"time"
)

type ConnPool struct {
	c              chan int
	mu             sync.Mutex
	Conns          map[int]*QProcess
	reservedConns  map[int]*QProcess
	Timeout        time.Duration
	ReviveInterval time.Duration
	DeadConns      map[int]bool
	AllowedAPI     map[string]bool
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
		pool.reservedConns = make(map[int]*QProcess)
	}
	pool.Conns[q.Port] = q
	pool.DeadConns[q.Port] = false
	qCopy := *q
	qCopy.Dial()
	pool.reservedConns[q.Port] = &qCopy
}

func (pool *ConnPool) Reload() {
	for _, q := range pool.reservedConns {
		if q.conn == nil {
			q.Dial()
		}
		err := q.Async([]byte("\\l ."))
		if err != nil {
			log.Printf("geek:Reload failed to reload %d, %s", q.Port, err)
		}
	}
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
	defer func() {
		if err := recover(); err != nil {
			log.Println("geek.Sync failed: ", err)
		}
	}()
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
	return ErrMaxRetryTimesReached
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

func (pool *ConnPool) Validate(API string) error {
	if pool.AllowedAPI == nil {
		return nil
	} else {
		if _, ok := pool.AllowedAPI[API]; ok {
			return nil
		} else {
			return ErrNotAllowedAPI
		}
	}
}

func (pool *ConnPool) Handle(qClient *QProcess) (int64, int64, string, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("geek.Handle failed: ", err)
		}
	}()
	pool.increase()
	defer pool.decrease()

	deadline := time.Now().Add(pool.Timeout)
	qClient.conn.SetDeadline(deadline)
	inputSize, err := qClient.peekMsgLength()
	if err != nil {
		qClient.Close()
		return 0, 0, "", err
	}
	// for retrying
	input := make([]byte, inputSize)
	// cached input for retrying
	qClient.reader.Read(input)
	input = Decompress(input)
	api := PeekAPI(input)
	err = pool.Validate(api)
	if err != nil {
		if api == "" {
			qClient.Err(ErrInvalidQuery)
			return 0, 0, api, ErrInvalidQuery
		} else {
			qClient.Err(ErrNotAllowedAPI)
			return 0, 0, api, ErrNotAllowedAPI
		}
	}
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
			return inputSize, 0, api, err
		}
		_, err = io.CopyN(qClient.writer, q.reader, outputSize)
		if err != nil {
			// read error
			if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
				pool.DeadConns[port] = true
				continue
			} else {
				pool.c <- port
				return inputSize, outputSize, api, err
			}
		}
		pool.c <- q.Port
		return inputSize, outputSize, api, nil
	}

	qClient.Err(ErrMaxRetryTimesReached)
	return inputSize, 0, api, ErrMaxRetryTimesReached
}
