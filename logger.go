package geek

import (
	"fmt"
	"io"
	"time"
)

type LogParams struct {
	TimeStamp    time.Time
	Duration     time.Duration
	Status       string
	User         string
	ClientIP     string
	API          string
	InputSize    int64
	OutputSize   int64
	ErrorMsg     string
	QueueCounter int
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
	return fmt.Sprintf(
		"[GEEK] %v | %9v | %9v | %15s | %20s | i/o %.2fKB %.2fMB | %5d | %20s | %s\n",
		param.TimeStamp.Format("2006.01.02D15:04:05"),
		param.Status,
		param.Duration,
		param.User,
		param.ClientIP,
		float32(param.InputSize)/1024,
		float32(param.OutputSize)/1048576,
		param.QueueCounter,
		param.API,
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

		inputSize, outputSize, api, err := p.Handle(q)

		param := LogParams{
			User:         q.User,
			ClientIP:     clientIP,
			API:          api,
			InputSize:    inputSize,
			OutputSize:   outputSize,
			QueueCounter: p.GetQueueCounter(),
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
