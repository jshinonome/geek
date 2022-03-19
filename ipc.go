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
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ASYNC byte = iota
	SYNC
	RESPONSE
)

const (
	K0   byte = 0
	KB   byte = 1
	KJ   byte = 7
	KF   byte = 9
	KC   byte = 10
	KS   byte = 11
	KP   byte = 12
	kErr byte = 128
	kb   byte = 255
	kj   byte = 249
	kf   byte = 247
	kc   byte = 246
	ks   byte = 245
	kp   byte = 244
	XT   byte = 98
	XD   byte = 99
)

const (
	Wj = math.MaxInt64
	Nj = math.MinInt64
)

var validType = map[byte]bool{
	K0:   true,
	KB:   true,
	KJ:   true,
	KF:   true,
	KC:   true,
	KS:   true,
	KP:   true,
	kb:   true,
	kj:   true,
	kf:   true,
	kc:   true,
	ks:   true,
	kp:   true,
	XT:   true,
	XD:   true,
	kErr: true,
}

var (
	rb = reflect.TypeOf(false)
	rj = reflect.TypeOf(int64(0))
	rf = reflect.TypeOf(float64(0))
	rc = reflect.TypeOf(byte(0))
	rs = reflect.TypeOf("")
	rt = reflect.TypeOf(time.Time{})
	rp = reflect.TypeOf(timestamppb.Timestamp{})
	RB = reflect.TypeOf([]bool{})
	RJ = reflect.TypeOf([]int64{})
	RF = reflect.TypeOf([]float64{})
	RC = reflect.TypeOf([]byte{})
	RS = reflect.TypeOf([]string{})
	RT = reflect.TypeOf([]time.Time{})
	RP = reflect.TypeOf([]timestamppb.Timestamp{})
	R0 = reflect.TypeOf([][]byte{})
)
var reflectTypeToKType = map[reflect.Type]byte{
	rb: kb,
	rj: kj,
	rf: kf,
	rc: kc,
	rs: ks,
	rt: kp,
	rp: kp,
	RB: KB,
	RJ: KJ,
	RF: KF,
	RC: KC,
	RS: KS,
	RT: KP,
	RP: KP,
	R0: K0,
}

var (
	Nf = math.NaN()
	Wf = math.Inf(1)
)

var kLen = map[byte]int{
	kb: 1,
	kj: 8,
	kc: 1,
	kp: 8,
	kf: 8,
}

// time in kdb+ starts from 2000.01.01, time in go starts from 1970.01.01
const nanoDiff int64 = 946684800000000000

var emptyList [6]byte

func calcLen(v reflect.Value) (int, error) {
	kType, ok := reflectTypeToKType[v.Type()]
	if ok {
		switch kType {
		case kb, kc, kj, kf, kp:
			return kLen[kType] + 1, nil
		case ks:
			return v.Len() + 2, nil
		case KB, KC, KJ, KF, KP:
			return kLen[0-kType]*v.Len() + 6, nil
		case KS:
			length := 6
			for i := 0; i < v.Len(); i++ {
				length += len(v.Index(i).Interface().(string)) + 1
			}
			return length, nil
		// [][]byte
		case K0:
			length := 6
			for i := 0; i < v.Len(); i++ {
				l, err := calcLen(v.Index(i))
				if err != nil {
					return 0, err
				}
				length += l
			}
			return length, nil
		}
	}

	switch v.Kind() {
	// dict, treat keys and values as mixed lists
	case reflect.Map:
		if v.Len() == 0 {
			return len(emptyList)*2 + 1, nil
		} else {
			m := v.MapRange()
			len := 0
			for i := 0; m.Next(); i++ {
				keyLen, err := calcLen(m.Key())
				if err != nil {
					return 0, err
				}
				valueLen, err := calcLen(m.Value())
				if err != nil {
					return 0, err
				}
				len += keyLen + valueLen
			}
			return len + 13, nil
		}
	// dict or mixed
	case reflect.Struct:
		if v.NumField() == 0 {
			return 0, &GeekErr{"geek:calcLen not support empty struct"}
		}
		// first tag, decide to be a mixed list or a dict
		firstTag := v.Type().Field(0).Tag.Get("k")
		isMixed := firstTag == ""
		var keyLen, valueLen int
		for i := 0; i < v.NumField(); i++ {
			tag := v.Type().Field(i).Tag.Get("k")
			if tag == "" && !isMixed {
				return 0, &GeekErr{"geek:calcLen undefined k tag for " + v.Type().Field(i).Name}
			}
			keyLen += len(tag) + 1
			l, err := calcLen(v.Field(i))
			if err != nil {
				return 0, err
			}
			valueLen += l
		}
		if isMixed {
			return valueLen + 6, nil
		} else {
			return keyLen + valueLen + 13, nil
		}
	// table
	case reflect.Slice:
		rowItem := reflect.MakeSlice(v.Type(), 1, 1)
		// get struct
		rowStruct := rowItem.Index(0)
		if rowStruct.Kind() != reflect.Struct {
			break
		}
		if rowStruct.NumField() == 0 {
			return 0, &GeekErr{"geek:calcLen not suppport slice of empty struct"}
		}
		fieldLen := 6
		for i := 0; i < rowStruct.NumField(); i++ {
			tag := rowStruct.Type().Field(i).Tag.Get("k")
			if len(tag) == 0 {
				return 0, &GeekErr{"geek:calcLen undefined k tag for " + rowStruct.Type().Field(i).Name}
			}
			fieldLen += len(tag) + 1
		}
		// empty table ipc length
		length := fieldLen + rowStruct.NumField()*6 + 9
		if v.Len() == 0 {
			return length, nil
		}
		for i := 0; i < rowStruct.NumField(); i++ {
			fieldType := rowStruct.Type().Field(i).Type
			kType := reflectTypeToKType[fieldType]
			if kType == 0 {
				return 0, &GeekErr{"geek:calcLen not suppport field type: " + fieldType.String()}
			}
			fixedLen, ok := kLen[kType]
			if ok {
				length += fixedLen * v.Len()
			} else {
				// unfixed length k types
				for j := 0; j < v.Len(); j++ {
					l, err := calcLen(v.Index(j).Field(i))
					if err != nil {
						return 0, err
					}
					if v.Index(j).Field(i).Kind() == reflect.String {
						l -= 1
					}
					length += l
				}
			}
		}
		return length, nil
	}
	return 0, &GeekErr{fmt.Sprintf("geek:calcLen nyi read type:%s", v.Type())}
}

// need a length calculate function, so I don't cashed the whole ipc message
func writeIPC(writer *bufio.Writer, msgType byte, k interface{}) error {
	v := reflect.ValueOf(k)
	length, err := calcLen(v)
	if err != nil {
		return err
	}
	// first byte means little endian
	writer.Write([]byte{1, msgType, 0, 0})
	binaryWriteLen(writer, length+8)
	// write k object
	err = writeK(writer, v)
	if err != nil {
		return err
	}
	return writer.Flush()
}

func writeErr(writer *bufio.Writer, err error) error {
	writer.Write([]byte{1, RESPONSE, 0, 0})
	length := len(err.Error()) + 10
	binaryWriteLen(writer, length)
	writer.WriteByte(kErr)
	writer.Write([]byte(err.Error()))
	writer.WriteByte(0)
	return writer.Flush()
}

func binaryWrite(writer *bufio.Writer, k interface{}) {
	binary.Write(writer, binary.LittleEndian, k)
}

func binaryWriteLen(writer *bufio.Writer, length int) {
	binary.Write(writer, binary.LittleEndian, uint32(length))
}

// string -> symbol, int64 -> long, float64 -> float, time -> timestamp, bool -> boolean, byte -> char
func writeK(writer *bufio.Writer, v reflect.Value) error {
	t := v.Type()
	kType, ok := reflectTypeToKType[t]
	if ok {
		writer.WriteByte(kType)
		switch kType {
		case kb, kc, kj, kf:
			binaryWrite(writer, v.Interface())
		case kp:
			binaryWrite(writer, v.Interface().(time.Time).UnixNano()-nanoDiff)
		case ks:
			binaryWrite(writer, []byte(v.Interface().(string)))
			writer.WriteByte(0)
		case KB, KC, KJ, KF:
			writer.WriteByte(0)
			binaryWriteLen(writer, v.Len())
			binaryWrite(writer, v.Interface())
		case KP:
			writer.WriteByte(0)
			binaryWriteLen(writer, v.Len())
			ns := make([]int64, v.Len())
			for i := 0; i < v.Len(); i++ {
				ns[i] = v.Index(i).Interface().(time.Time).UnixNano() - nanoDiff
			}
			binaryWrite(writer, ns)
		case KS:
			writer.WriteByte(0)
			binaryWriteLen(writer, v.Len())
			for i := 0; i < v.Len(); i++ {
				binaryWrite(writer, []byte(v.Index(i).Interface().(string)))
				writer.WriteByte(0)
			}
		case K0:
			// [][]byte
			writer.WriteByte(0)
			binaryWriteLen(writer, v.Len())
			for i := 0; i < v.Len(); i++ {
				err := writeK(writer, v.Index(i))
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	switch v.Kind() {
	// dict, treat keys and values as mixed lists
	case reflect.Map:
		writer.WriteByte(99)
		if v.Len() == 0 {
			writer.Write(emptyList[:])
			writer.Write(emptyList[:])
		} else {
			m := v.MapRange()
			m.Next()
			values := reflect.MakeSlice(reflect.SliceOf(m.Value().Type()), v.Len(), v.Len())
			writer.WriteByte(0)
			writer.WriteByte(0)
			binaryWriteLen(writer, v.Len())
			err := writeK(writer, m.Key())
			if err != nil {
				return err
			}
			values.Index(0).Set(m.Value())
			for i := 1; m.Next(); i++ {
				writeK(writer, m.Key())
				values.Index(i).Set(m.Value())
			}
			writer.WriteByte(0)
			writer.WriteByte(0)
			binaryWriteLen(writer, v.Len())
			for i := 0; i < v.Len(); i++ {
				err := writeK(writer, values.Index(i))
				if err != nil {
					return err
				}
			}
		}
		return nil
	// dict or mixed
	case reflect.Struct:
		firstTag := v.Type().Field(0).Tag.Get("k")
		isMixed := firstTag == ""
		if !isMixed {
			writer.WriteByte(99)
			writer.WriteByte(11)
			writer.WriteByte(0)
			binaryWrite(writer, uint32(v.NumField()))
			for i := 0; i < v.NumField(); i++ {
				tag := v.Type().Field(i).Tag.Get("k")
				writer.Write([]byte(tag))
				writer.WriteByte(0)
			}
		}
		writer.WriteByte(0)
		writer.WriteByte(0)
		binaryWrite(writer, uint32(v.NumField()))
		for i := 0; i < v.NumField(); i++ {
			err := writeK(writer, v.Field(i))
			if err != nil {
				return err
			}
		}
		return nil
	// table
	case reflect.Slice:
		rowItem := reflect.MakeSlice(v.Type(), 1, 1)
		// get struct
		rowStruct := rowItem.Index(0)
		// table, skip, dict, syms, skip
		writer.Write([]byte{98, 0, 99, 11, 0})
		binaryWriteLen(writer, rowStruct.NumField())
		for i := 0; i < rowStruct.NumField(); i++ {
			tag := rowStruct.Type().Field(i).Tag.Get("k")
			writer.Write([]byte(tag))
			writer.WriteByte(0)
		}
		// mixed list, skip
		writer.Write([]byte{0, 0})
		binaryWriteLen(writer, rowStruct.NumField())
		for i := 0; i < rowStruct.NumField(); i++ {
			column := reflect.MakeSlice(reflect.SliceOf(rowStruct.Type().Field(i).Type), v.Len(), v.Len())
			for j := 0; j < v.Len(); j++ {
				column.Index(j).Set(v.Index(j).Field(i))
			}
			err := writeK(writer, column)
			if err != nil {
				return err
			}
		}
		return nil
	}
	return &GeekErr{fmt.Sprintf("geek:WriteK:nyi read type:%s, kind:%s", v.Type(), v.Kind())}
}

func readIPCLen(reader *bufio.Reader) (int, error) {
	header := make([]byte, 8)
	_, err := reader.Read(header)
	if err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint32(header[4:])), nil
}

func readIPC(reader *bufio.Reader, k interface{}) error {
	length, err := readIPCLen(reader)
	if err != nil {
		return err
	}
	readLen := 8
	defer func() {
		if readLen < length {
			reader.Discard(length - readLen)
		}
	}()
	rv := reflect.ValueOf(k)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &GeekErr{"geek:readIPC require a non-nil pointer type"}
	}
	err = readK(reader, &readLen, rv)
	if err != nil {
		return err
	}
	return nil
}

func readK(reader *bufio.Reader, readLen *int, v reflect.Value) error {
	if v.Kind() != reflect.Ptr {
		return &GeekErr{"geek:readK require a pointer type"}
	}
	pv := v.Elem()
	t := pv.Type()
	kType, err := reader.ReadByte()
	*readLen += 1
	if err != nil {
		return err
	}
	if !validType[kType] {
		return &GeekErr{fmt.Sprintf("geek:readK unsupported k type:%v", int8(kType))}
	}
	// error returned
	if kType == kErr {
		k, err := reader.ReadBytes(0)
		*readLen += len(k)
		if err != nil {
			return err
		}
		return &GeekErr{"`" + string(k[:len(k)-1])}
	}
	targetType, ok := reflectTypeToKType[t]
	if ok {
		if targetType != kType {
			return &GeekErr{fmt.Sprintf("geek:readK nyi type:%s, kType:%d", t, int8(kType))}
		}
	}
	// handle atom or list
	if ok {
		switch targetType {
		case kb, kc, kj, kf:
			binaryRead(reader, v.Interface())
			*readLen += kLen[kType]
			return nil
		case ks:
			s, err := reader.ReadBytes(0)
			*readLen += len(s)
			if err != nil {
				return err
			}
			pv.SetString(string(s[:len(s)-1]))
			return nil
		case kp:
			var i int64
			binaryRead(reader, &i)
			*readLen += kLen[kType]
			if t == rt {
				pv.Set(reflect.ValueOf(time.Unix(0, i+nanoDiff).UTC()))
			} else {
				pv.Set(reflect.ValueOf(
					timestamppb.Timestamp{
						Seconds: (i + nanoDiff) / 1000_000_000,
						Nanos:   int32(i % 1000_000_000),
					}))
			}
			return nil
		case KB, KC, KJ, KF:
			// skip attr
			reader.ReadByte()
			length := readLength(reader)
			*readLen += 5
			list := reflect.MakeSlice(pv.Type(), length, length).Interface()
			binaryRead(reader, list)
			pv.Set(reflect.ValueOf(list))
			*readLen += length * kLen[^kType+1]
			return nil
		case KS:
			// skip attr
			reader.ReadByte()
			length := readLength(reader)
			*readLen += 5
			strings := make([]string, length)
			for i := 0; i < length; i++ {
				s, err := reader.ReadBytes(0)
				*readLen += len(s)
				if err != nil {
					return err
				}
				strings[i] = string(s[:len(s)-1])
			}
			pv.Set(reflect.ValueOf(strings))
			return nil
		case KP:
			// skip attr
			reader.ReadByte()
			length := readLength(reader)
			*readLen += 5
			var j = make([]int64, length)
			binaryRead(reader, j)
			*readLen += length * kLen[kj]
			if t == rt {
				times := make([]time.Time, length)
				for i, ns := range j {
					times[i] = time.Unix(0, ns+nanoDiff).UTC()
				}
				pv.Set(reflect.ValueOf(times))
			} else {
				times := make([]timestamppb.Timestamp, length)
				for i, ns := range j {
					times[i] = timestamppb.Timestamp{
						Seconds: (ns + nanoDiff) / 1000_000_000,
						Nanos:   int32(ns % 1000_000_000),
					}
				}
				pv.Set(reflect.ValueOf(times))
			}
			return nil
		}
	}

	switch pv.Kind() {
	// dict -> map
	case reflect.Map:
		if kType != 99 {
			break
		}
		keys := reflect.New(reflect.SliceOf(pv.Type().Key()))
		err := readK(reader, readLen, keys)
		if err != nil {
			return err
		}
		values := reflect.New(reflect.SliceOf(pv.Type().Elem()))
		err = readK(reader, readLen, values)
		if err != nil {
			return err
		}
		m := reflect.MakeMap(pv.Type())
		for i := 0; i < keys.Elem().Len(); i++ {
			m.SetMapIndex(keys.Elem().Index(i), values.Elem().Index(i))
		}
		pv.Set(m)
		return nil
	// dict | mixed list -> struct
	case reflect.Struct:
		if kType != 99 && kType != 0 {
			break
		}
		var valueType byte
		if kType == 99 {
			keys := reflect.New(reflect.SliceOf(reflect.TypeOf("")))
			err := readK(reader, readLen, keys)
			if err != nil {
				return err
			}
			err = compareKeyWithStructField(keys, pv)
			if err != nil {
				return err
			}
			peekBytes, err := reader.Peek(1)
			if err != nil {
				return err
			}
			valueType = peekBytes[0]
		}

		if valueType != 0 {
			// all fields should be the same type
			fieldType := pv.Type().Field(0).Type
			for i := 0; i < pv.NumField(); i++ {
				if fieldType != pv.Type().Field(i).Type {
					return &GeekErr{fmt.Sprintf("geek:readK mismatch, struct:%s, k:%s", pv.Type().Field(i).Type, fieldType)}
				}
			}
			values := reflect.New(reflect.SliceOf(fieldType))
			err = readK(reader, readLen, values)
			if err != nil {
				return err
			}
			if values.Elem().Len() != pv.NumField() {
				return &GeekErr{fmt.Sprintf("geek:readK length, struct:%d, k:%d", pv.NumField(), values.Elem().Len())}
			}
			for i := 0; i < pv.NumField(); i++ {
				pv.Field(i).Set(values.Elem().Index(i))
			}
		} else {
			// mixed list, skip 1 byte if just read keys
			if kType == 99 {
				// skip kType
				reader.ReadByte()
				*readLen += 5
			}
			// skip attribute
			reader.ReadByte()
			length := readLength(reader)
			*readLen += 5
			if length != pv.NumField() {
				return &GeekErr{fmt.Sprintf("geek:readK length, struct:%d, k:%d", pv.NumField(), length)}
			}
			for i := 0; i < pv.NumField(); i++ {
				value := reflect.New(pv.Type().Field(i).Type)
				err := readK(reader, readLen, value)
				if err != nil {
					return err
				}
				pv.Field(i).Set(value.Elem())
			}
		}
		return nil
	// slice of struct -> table
	case reflect.Slice:
		rowItem := reflect.MakeSlice(pv.Type(), 1, 1)
		// get struct
		rowStruct := rowItem.Index(0)
		if rowStruct.Kind() != reflect.Struct {
			break
		}
		if kType != 98 {
			break
		}
		// skip 0
		reader.ReadByte()
		// skip 99
		reader.ReadByte()
		*readLen += 2
		// read headers
		keys := reflect.New(reflect.SliceOf(reflect.TypeOf("")))
		err := readK(reader, readLen, keys)
		if err != nil {
			return err
		}
		err = compareKeyWithStructField(keys, rowStruct)
		if err != nil {
			return err
		}
		// skip count of columns
		reader.Read(make([]byte, 6))
		*readLen += 6
		peekBytes, err := reader.Peek(6)
		if err != nil {
			return err
		}
		length := int(binary.LittleEndian.Uint32(peekBytes[2:6]))
		allItems := reflect.MakeSlice(pv.Type(), length, length)
		// read by fields
		for i := 0; i < rowStruct.NumField(); i++ {
			fieldValues := reflect.New(reflect.SliceOf(rowStruct.Type().Field(i).Type))
			err = readK(reader, readLen, fieldValues)
			if err != nil {
				return err
			}
			for j := 0; j < length; j++ {
				allItems.Index(j).Field(i).Set(fieldValues.Elem().Index(j))
			}
		}
		pv.Set(allItems)
		return nil
	}
	return &GeekErr{fmt.Sprintf("geek:readK nyi target type:%s, k type:%d", t, int8(kType))}
}

func binaryRead(reader *bufio.Reader, k interface{}) {
	binary.Read(reader, binary.LittleEndian, k)
}

func readLength(reader *bufio.Reader) int {
	var length uint32
	binary.Read(reader, binary.LittleEndian, &length)
	return int(length)
}

// keys is a *[]string, pv is a struct
func compareKeyWithStructField(keys reflect.Value, pv reflect.Value) error {
	if keys.Elem().Len() != pv.NumField() {
		return &GeekErr{fmt.Sprintf("geek:compareKeyWithStructField length, struct:%d, k:%d", pv.NumField(), keys.Elem().Len())}
	} else if keys.Elem().Type() != reflect.SliceOf(reflect.TypeOf("")) {
		return &GeekErr{fmt.Sprintf("geek:compareKeyWithStructField not symbol key, k:%d", keys.Elem().Type())}
	}
	for i := 0; i < pv.NumField(); i++ {
		key := keys.Elem().Index(i).Interface().(string)
		tag := pv.Type().Field(i).Tag.Get("k")
		if key != tag {
			return &GeekErr{fmt.Sprintf("geek:compareKeyWithStructField mismatch, struct tag:%s ,k:%s", tag, key)}
		}
	}
	return nil
}

func Compress(msg []byte) []byte {
	// skip compression if < 4MB
	if len(msg) < 2000 {
		return msg
	}
	cMaxLen := len(msg) / 2
	cMsg := make([]byte, cMaxLen)
	copy(cMsg, msg[:4])
	// set to compressed
	cMsg[2] = 1
	// original msg length
	copy(cMsg[8:12], msg[4:8])
	// compressed msg position
	cPos := 12
	nPos := cPos
	// original position & length
	oPos := 8
	oLen := len(msg)

	// cached xor value for oPos and oPos + 1
	X := make([]int, 256)
	// px, x of previous loop
	// compression counter, the number of 1 in n binary format
	var px, n byte
	var pPos int
	for i := byte(0); oPos < oLen; i <<= 1 {
		if i == 0 {
			if cPos > cMaxLen-17 {
				return msg
			}
			i = 1
			cMsg[nPos] = n
			nPos = cPos
			cPos++
			n = 0
		}

		skip := oLen-oPos < 3
		var xPos int
		var x byte
		if !skip {
			x = msg[oPos] ^ msg[oPos+1]
			xPos = X[x]
			// if same x exist and the start bytes equal, not skip,
			// 2+ bytes are the same(duplicate)
			skip = xPos == 0 || msg[oPos] != msg[xPos]
		}

		// update cached X from previous result
		// update here to enforce 2+ bytes duplicate
		if pPos > 0 {
			X[px] = pPos
			pPos = 0
		}

		// update new position for x
		if skip {
			px = x
			pPos = oPos
			// copy 1 byte and continue
			cMsg[cPos] = msg[oPos]
			cPos++
			oPos++
		} else {
			X[x] = oPos
			n |= i
			xPos += 2
			oPos += 2
			// start of duplicate
			s := oPos
			max := oPos + 255
			if oPos+255 > oLen {
				max = oLen
			}
			for ; oPos < max && msg[xPos] == msg[oPos]; oPos++ {
				xPos++
			}
			cMsg[cPos] = x
			cPos++
			cMsg[cPos] = byte(oPos - s)
			cPos++
		}
	}
	cMsg[nPos] = n
	binary.LittleEndian.PutUint32(cMsg[4:8], uint32(cPos))
	return cMsg[:cPos:cPos]
}

func Decompress(cMsg []byte) []byte {
	if cMsg[2] == 0 {
		return cMsg
	}
	var oLen = int(binary.LittleEndian.Uint32(cMsg[8:12]))
	msg := make([]byte, oLen)
	copy(msg[:4], cMsg[:4])
	msg[2] = 0
	copy(msg[4:8], cMsg[8:12])
	oPos := 8
	xPos := oPos
	cPos := 12
	X := make([]int, 256)
	var n byte
	for i := byte(0); oPos < oLen; i <<= 1 {
		if i == 0 {
			n = cMsg[cPos]
			cPos++
			i = 1
		}
		var r int
		if n&i != 0 {
			s := X[cMsg[cPos]]
			cPos++
			// the count of repeated bytes
			r = int(cMsg[cPos])
			cPos++
			for j := 0; j < r+2; j++ {
				msg[oPos+j] = msg[s+j]
			}
			// to update xPos for last r bytes, so DON'T add r here
			oPos += 2
		} else {
			msg[oPos] = cMsg[cPos]
			oPos++
			cPos++
		}
		// cache xor value
		for ; xPos < oPos-1; xPos++ {
			X[msg[xPos]^msg[xPos+1]] = xPos
		}

		if n&i != 0 {
			oPos += r
			xPos = oPos
		}
	}
	return msg
}
