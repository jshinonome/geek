# Geek

Geek is a kdb+/q interface for Go (Golang). It comes with a kdb+/q load balancer ConnPool, which uses a buffered channel to handle the connections pool.

-   ConnPool.Handle
    -   doesn't serialize or deserialize IPC message.
    -   before passing on IPC messages, define .geek.user and .geek.ip on the q process
-   ConnPool.Sync
    -   need a known go type pointer as an parameter

## Contents

-   [Installation](#installation)
-   [Quick Start](#quick-start)
-   [Data Types](#data-types)

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

2. Start a q process

```q
q -p 1800
trade:([]time:"p"$.z.D-til 10;sym:10?`a`b;price:10?10.0;qty:10?10);
getTrade:{select from trade where sym=x};
.z.pg:{[x]0N!(`zpg;x);value x};
```

3. Run go program

```sh
go run example.go
```

## Data Types

### Basic Types

Intend to support only these 6 types

| kdb       | go        |
| --------- | --------- |
| long      | int64     |
| float     | float64   |
| char      | byte      |
| symbol    | string    |
| timestamp | time.Time |
| boolean   | bool      |

### Other Types

go struct without k tags -> k mixed list

```go
struct {
	Func   string
	Param1 string
	Param2 string
}
```

go map -> k dictionary

```go
map[string]bool
```

go struct with k tags(keys' name) -> k dictionary

```go
struct {
	Key1 string    `k:"sym"`
	Key2 time.Time `k:"time"`
	Key3 []byte    `k:"qty"`
}
```

go list of a struct with k tags(column names) -> k table

```go
[]struct {
	Key1 string    `k:"sym"`
	Key2 time.Time `k:"time"`
	Key3 []byte    `k:"qty"`
}
```
