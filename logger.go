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
	"fmt"
	"io"
	"time"
)

type LogParams struct {
	TimeStamp  time.Time
	Duration   time.Duration
	Status     string
	User       string
	ClientIP   string
	API        string
	InputSize  int64
	OutputSize int64
	ErrorMsg   string
}

type LogFormatter func(params LogParams) string

type LoggerConfig struct {
	// Optional. Default value is geek.defaultLogFormatter
	Formatter LogFormatter

	// Output is a writer where logs are written.
	// Optional. Default value is geek.DefaultWriter.
	Output io.Writer
}

var defaultLogFormatter = func(param LogParams) string {
	return fmt.Sprintf("[GEEK] %v | %9v | %9v | %15s | %20s | i %.2fKB | o %.2fMB \n%s",
		param.TimeStamp.Format("2006.01.02D15:04:05"),
		param.Status,
		param.Duration,
		param.User,
		param.ClientIP,
		float32(param.InputSize)/1024,
		float32(param.OutputSize)/1048576,
		param.ErrorMsg,
	)
}

// LoggerWithConfig instance a Logger middleware with config.
func LoggerWithConfig(conf LoggerConfig) HandlerFunc {
	formatter := conf.Formatter
	if formatter == nil {
		formatter = defaultLogFormatter
	}

	out := conf.Output
	if out == nil {
		out = DefaultLogWriter
	}
	return func(p *ConnPool, q *QProcess) {
		start := time.Now()
		clientIP := q.conn.RemoteAddr().String()
		api, _ := q.PeekAPI()

		inputSize, outputSize, err := p.Handle(q)

		param := LogParams{
			User:       q.User,
			ClientIP:   clientIP,
			API:        api,
			InputSize:  inputSize,
			OutputSize: outputSize,
		}
		if err != nil {
			param.Status = "Failed"
			param.ErrorMsg = err.Error()
		} else {
			param.Status = "Succeeded"
		}

		// Stop timer
		param.TimeStamp = time.Now()
		param.Duration = param.TimeStamp.Sub(start)
		fmt.Fprint(out, formatter(param))
	}
}
