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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var readWriteCases = []struct {
	k         interface{}
	name      string
	ipcFormat []byte
}{
	{true, "boolean: 1b", []byte{255, 1}},
	{byte('a'), "char: 'a'", []byte{246, 97}},
	{"test", "symbol: `test", []byte{245, 116, 101, 115, 116, 0}},
	{int64(1), "long: 1", []byte{249, 1, 0, 0, 0, 0, 0, 0, 0}},
	{float64(1), "float: 1.0", []byte{247, 0, 0, 0, 0, 0, 0, 240, 63}},
	{time.Date(2022, 1, 23, 0, 0, 0, 0, time.UTC), "timestamp: 2022.01.23D", []byte{244, 0, 0, 166, 208, 101, 112, 169, 9}},
	{[]bool{true, false}, "booleans: 10b", []byte{1, 0, 2, 0, 0, 0, 1, 0}},
	{[]byte("ab"), "chars: 'ab'", []byte{10, 0, 2, 0, 0, 0, 97, 98}},
	{[]string{"test", "symbol"}, "symbols: `test`symbol", []byte{11, 0, 2, 0, 0, 0, 116, 101, 115, 116, 0, 115, 121, 109, 98, 111, 108, 0}},
	{[]int64{1, 2}, "longs: 1 2", []byte{7, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}},
	{[]float64{1.0, 2.0}, "floats: 1.0 2.0", []byte{9, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}},
	{[]time.Time{time.Date(2022, 1, 23, 0, 0, 0, 0, time.UTC),
		time.Date(2022, 1, 24, 0, 0, 0, 0, time.UTC)},
		"timestamps: 2022.01.23D 2022.01.24D",
		[]byte{12, 0, 2, 0, 0, 0, 0, 0, 166, 208, 101, 112, 169, 9, 0, 0, 245, 97, 250, 190, 169, 9}},
	{[]bool{}, "empty boolean list", []byte{1, 0, 0, 0, 0, 0}},
	{[]byte{}, "empty char list", []byte{10, 0, 0, 0, 0, 0}},
	{[]string{}, "empty symbol list", []byte{11, 0, 0, 0, 0, 0}},
	{[]int64{}, "empty long list", []byte{7, 0, 0, 0, 0, 0}},
	{[]float64{}, "empty float list", []byte{9, 0, 0, 0, 0, 0}},
	{[]time.Time{}, "empty timestamp list", []byte{12, 0, 0, 0, 0, 0}},
	{struct {
		A int64
		B bool
	}{1, true}, "mixed: (1;1b)", []byte{0, 0, 2, 0, 0, 0, 249, 1, 0, 0, 0, 0, 0, 0, 0, 255, 1}},
	{jb{1, true}, "dict: `a`b!(1;1b)", []byte{99, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 0, 0, 2, 0, 0, 0, 249, 1, 0, 0, 0, 0, 0, 0, 0, 255, 1}},
	// map has no order, so only use 1 key here
	{map[string]int64{"abc": 1}, "dict: ((),`abc)!enlist 1", []byte{99, 0, 0, 1, 0, 0, 0, 245, 97, 98, 99, 0, 0, 0, 1, 0, 0, 0, 249, 1, 0, 0, 0, 0, 0, 0, 0}},
	{[]jb{{1, true}}, "table: ,`a`b!(1;1b)", []byte{98, 0, 99, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 0, 0, 2, 0, 0, 0, 7, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1}},
	{[]jb{}, "empty table: +`a`b!\"JB\"$\\:()", []byte{98, 0, 99, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 0, 0, 2, 0, 0, 0, 7, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0}},
	{[]stC{{"7203.T", time.Date(2022, 2, 19, 0, 0, 0, 0, time.UTC), []byte("oa")}}, "table: ,`sym`time`tag!(`7203.T;2022.02.19D;\"oa\")", []byte{98, 0, 99, 11, 0, 3, 0, 0, 0, 115, 121, 109, 0, 116, 105, 109, 101, 0, 116, 97, 103, 0, 0, 0, 3, 0, 0, 0, 11, 0, 1, 0, 0, 0, 55, 50, 48, 51, 46, 84, 0, 12, 0, 1, 0, 0, 0, 0, 0, 251, 35, 17, 186, 177, 9, 0, 0, 1, 0, 0, 0, 10, 0, 2, 0, 0, 0, 111, 97}},
	{[]stC{}, "table: ,`sym`time`tag!\"sp*\"$\\:()", []byte{98, 0, 99, 11, 0, 3, 0, 0, 0, 115, 121, 109, 0, 116, 105, 109, 101, 0, 116, 97, 103, 0, 0, 0, 3, 0, 0, 0, 11, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
}

func TestWriteK(t *testing.T) {
	for _, test := range readWriteCases {
		fmt.Println("Test write - " + test.name)
		buf := new(bytes.Buffer)
		writer := bufio.NewWriter(buf)
		v := reflect.ValueOf(test.k)
		length, err := calcLen(v)
		if err != nil {
			t.Errorf("Failed to calculate length - %v", err.Error())
		}
		if diff := cmp.Diff(len(test.ipcFormat), length); diff != "" {
			t.Error(diff)
		}
		writeK(writer, v)
		writer.Flush()
		if diff := cmp.Diff(test.ipcFormat, buf.Bytes()); diff != "" {
			t.Error(diff)
		}
	}
}

func assert(expect interface{}, actual interface{}, t *testing.T) {
	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Error(diff)
	}
}

func TestReadIPC(t *testing.T) {
	q := QProcess{Port: qProcessPort, User: "test", Password: "test"}
	q.Dial()
	defer q.Close()

	// boolean
	ipcMsg := []byte("1b")
	fmt.Printf("Test read - %s\n", ipcMsg)
	var b bool
	q.Sync(&b, ipcMsg)
	assert(true, b, t)
	// booleans
	ipcMsg = []byte("10b")
	fmt.Printf("Test read - %s\n", ipcMsg)
	var B []bool
	q.Sync(&B, ipcMsg)
	assert([]bool{true, false}, B, t)
	// empty booleans
	ipcMsg = []byte("0#1b")
	fmt.Printf("Test read - %s\n", ipcMsg)
	var B0 []bool
	q.Sync(&B0, ipcMsg)
	assert([]bool{}, B0, t)

	ipcMsg = []byte("2022.01.25D12:34:56.789")
	fmt.Printf("Test read - go time %s\n", ipcMsg)
	var p time.Time
	q.Sync(&p, ipcMsg)
	if diff := cmp.Diff(time.Date(2022, 1, 25, 12, 34, 56, 789_000_000, time.UTC), p); diff != "" {
		t.Error(diff)
	}

	ipcMsg = []byte("til 3")
	fmt.Printf("Test read - %s\n", ipcMsg)
	int64s := make([]int64, 0)
	q.Sync(&int64s, ipcMsg)
	if diff := cmp.Diff([]int64{0, 1, 2}, int64s); diff != "" {
		t.Error(diff)
	}

	ipcMsg = []byte("10000#0")
	fmt.Printf("Test read - %s\n", ipcMsg)
	zeros := make([]int64, 0)
	q.Sync(&zeros, ipcMsg)
	if diff := cmp.Diff(make([]int64, 10000), zeros); diff != "" {
		t.Error(diff)
	}

	ipcMsg = []byte("2022.03.19D12:34:56.789")
	fmt.Printf("Test read - gRPC timestamp %s\n", ipcMsg)
	expectP := timestamppb.Timestamp{
		Seconds: 1647693296,
		Nanos:   789_000_000,
	}
	var actualP timestamppb.Timestamp
	q.Sync(&actualP, ipcMsg)
	if diff := cmp.Diff(actualP, expectP, cmpopts.IgnoreUnexported(timestamppb.Timestamp{})); diff != "" {
		t.Error(diff)
	}

	ipcMsg = []byte("1 2! 1 2")
	fmt.Printf("Test read - %s\n", ipcMsg)
	dictInt64 := make(map[int64]int64)
	q.Sync(&dictInt64, ipcMsg)
	expectDictInt64 := map[int64]int64{
		1: 1,
		2: 2,
	}
	if diff := cmp.Diff(expectDictInt64, dictInt64); diff != "" {
		t.Error(diff)
	}

	ipcMsg = []byte("`a`b!1 2")
	fmt.Printf("Test read - %s\n", ipcMsg)
	actualD1 := make(map[string]int64)
	q.Sync(&actualD1, ipcMsg)
	expectD1 := map[string]int64{
		"a": 1,
		"b": 2,
	}
	if diff := cmp.Diff(expectD1, actualD1); diff != "" {
		t.Error(diff)
	}

	ipcMsg = []byte("`a`b!1 2")
	fmt.Printf("Test read - %s\n", ipcMsg)
	actualD2 := struct {
		A int64 `k:"a"`
		B int64 `k:"b"`
	}{}
	q.Sync(&actualD2, ipcMsg)
	expectD2 := struct {
		A int64 `k:"a"`
		B int64 `k:"b"`
	}{A: 1, B: 2}
	if diff := cmp.Diff(expectD2, actualD2); diff != "" {
		t.Error(diff)
	}

	ipcMsg = []byte("`a`b!(1;1b)")
	fmt.Printf("Test read - %s\n", ipcMsg)
	actualD3 := jb{}
	q.Sync(&actualD3, ipcMsg)
	expectD3 := jb{A: 1, B: true}
	if diff := cmp.Diff(expectD3, actualD3); diff != "" {
		t.Error(diff)
	}
	// mixed list
	ipcMsg = []byte("(1;1b)")
	fmt.Printf("Test read - %s\n", ipcMsg)
	actualL0 := struct {
		A int64
		B bool
	}{}
	q.Sync(&actualL0, ipcMsg)
	expectL0 := struct {
		A int64
		B bool
	}{A: 1, B: true}
	if diff := cmp.Diff(expectL0, actualL0); diff != "" {
		t.Error(diff)
	}
	// table
	ipcMsg = []byte("enlist `a`b!(1;1b)")
	fmt.Printf("Test read - %s\n", ipcMsg)
	actualT1 := make([]jb, 0)
	q.Sync(&actualT1, ipcMsg)
	expectT1 := []jb{
		{A: 1, B: true},
	}
	if diff := cmp.Diff(expectT1, actualT1); diff != "" {
		t.Error(diff)
	}
	// empty table
	ipcMsg = []byte("0#enlist `a`b!(1;1b)")
	fmt.Printf("Test read - %s\n", ipcMsg)
	actualT2 := make([]jb, 0)
	q.Sync(&actualT2, ipcMsg)
	expectT2 := []jb{}
	if diff := cmp.Diff(expectT2, actualT2); diff != "" {
		t.Error(diff)
	}
}

type jb struct {
	A int64 `k:"a"`
	B bool  `k:"b"`
}

type stC struct {
	S string    `k:"sym"`
	T time.Time `k:"time"`
	C []byte    `k:"tag"`
}

var discardTests = []struct {
	ipcMsg []byte
}{
	{[]byte("10000#1i")},
	{[]byte("(enlist `dict)!enlist 1000#`a`b!1 2")},
	{[]byte("(enlist `dict)!enlist 1000#`a`b!1 2i")},
	{[]byte("enlist (enlist `dict)!enlist 1000#`a`b!1 2i")},
	{[]byte("`a+1")},
}

func TestDiscardUnreadBytes(t *testing.T) {
	q := QProcess{Port: qProcessPort, User: "test", Password: "test"}
	q.Dial()
	defer q.Close()
	for _, test := range discardTests {
		fmt.Printf("Test  - %s\n", test.ipcMsg)
		var k byte
		err := q.Sync(&k, test.ipcMsg)
		if err == nil {
			t.Errorf("should fail here")
		}
		var done string
		q.Sync(&done, []byte("`done"))
		if done != "done" {
			t.Errorf("failed to discard unread bytes")
		}
	}
}

var compressTest = []string{
	"2000#1b",
	"til 1000",
	"1000#enlist `a`b!1 2",
}

func TestCompress(t *testing.T) {
	q := QProcess{Port: qProcessPort, User: "test", Password: "test"}
	q.Dial()
	msg := make([]byte, 0)
	for _, test := range compressTest {
		q.Sync(&msg, []byte("`char$-8!"+test))
		actualCMsg := Compress(msg)
		expectCMsg := make([]byte, 0)
		q.Sync(&expectCMsg, []byte("`char$-18!"+test))
		if diff := cmp.Diff(expectCMsg, actualCMsg); diff != "" {
			t.Error(diff)
		}
		actualMsg := Decompress(expectCMsg)
		if diff := cmp.Diff(msg, actualMsg); diff != "" {
			t.Error(diff)
		}
	}
}
