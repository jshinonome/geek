# Geek

Geek is a kdb+/q interface for Go (Golang). It comes with a kdb+/q load balancer ConnPool, which uses a buffered channel to manage the connections pool. Also, geek.Engine doesn't serialize or deserialize IPC message.

## Installation

1. Install Geek

```sh
go get -u github.com/jshinonome/geek
```

2. Import it in your code

```go
import "github.com/jshinonome/geek"
```

## Quick Start

### Load balancer

1. Create an example.go file as below

```go
package main

import "github.com/jshinonome/geek"

func main() {
	// connect to a q process @ 1800
	q1 := geek.QProcess{Port: 1800}
	q1.Dial()
	// connect to a q process @ 1801
	q2 := geek.QProcess{Port: 1801}
	q2.Dial()
	qConnPool := geek.NewConnPool()
	qConnPool.Put(&q1)
	qConnPool.Put(&q2)
	qConnPool.Serving()

	qEngine := geek.DefaultEngine(qConnPool)
	qEngine.Run() // listen and server on :8101
}
```

2. Start two q processes

```sh
q -p 1800
```

```sh
q -p 1801
```

3. Run the q Engine

```sh
go run example.go
```

4. Start a new q process and run queries(geek.Engine doesn't keep client connection, so don't try to open a handle)

```q
`::8101 ("system";"p") //1800
`::8101 ("system";"p") //1801
```

### Interface with q process

1. Create an example.go file as below

```go
package main

import (
	"fmt"
	"time"

	"github.com/jshinonome/geek"
)

func main() {
	// connect to a q process @ 1800
	q := geek.QProcess{Port: 1800}
	q.Dial()
	sym := "a"
	f := struct {
		Api string
		Sym string
	}{
		"getTrade", sym,
	}
	r := make([]trade, 0)
	err := q.Sync(&r, f)
	if err != nil {
		fmt.Println(err)
	}
	for _, t := range r {
		fmt.Printf("%+v\n", t)
	}
}

type trade struct {
	Time  time.Time `k:"time"`
	Sym   string    `k:"sym"`
	Price float64   `k:"price"`
	Qty   int64     `k:"qty"`
}
```

```q
q -p 1800
trade:([]time:"p"$.z.D-til 10;sym:10?`a`b;price:10?10.0;qty:10?10);
getTrade:{select from trade where sym=x};
.z.pg:{[x]0N!(`zpg;x);value x};
```

```sh
go run example.go
```
