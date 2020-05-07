// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package resp

import (
	"bytes"
	"fmt"
)

type RESPType int

// RESP types
const (
	WrongType = iota // wrong input
	SimpleStringType
	ErrorType
	IntegerType
	BulkStringType
	ArrayType
)

// Value interface: WriteTo
type Value interface {
	WriteTo(*bytes.Buffer) error
}

// RESP SimpleString
type SimpleString string

func (s SimpleString) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", s)
	return err
}

// String
func (s SimpleString) String() string {
	return string(s)
}

// OkSimpleString & PongSimpleString
const OkSimpleString = SimpleString("OK")
const PongSimpleString = SimpleString("PONG")

// RESP Integer
type Integer int64

func (i Integer) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, ":%d\r\n", i)
	return err
}

// Three const integer: 0, 1, -1
const (
	ZeroInteger        = Integer(0)
	OneInteger         = Integer(1)
	NegativeOneInteger = Integer(-1)
)

// RESP Error
type Error string

func (e Error) Error() string {
	return string(e)
}

func (e Error) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, "-%s\r\n", e)
	return err
}

// NewError generates RESP Error
func NewError(format string, v ...interface{}) Error {
	return Error(fmt.Sprintf(format, v...))
}

// RESP BulkString
type BulkString []byte

func (b BulkString) WriteTo(w *bytes.Buffer) error {
	if b == nil {
		_, err := fmt.Fprintf(w, "$-1\r\n")
		return err
	}

	if _, err := fmt.Fprintf(w, "$%d\r\n", len(b)); err != nil {
		return err
	}

	w.Write(b)
	w.WriteString("\r\n")

	return nil
}

func (b BulkString) String() string {
	if b == nil {
		return ""
	}

	return string(b)
}

var (
	NilBulkString   = BulkString(nil)
	EmptyBulkString = BulkString([]byte(""))
)

// RESP Array
type Array []Value

// EmptyArray
var EmptyArray = Array{}

// WriteTo buffer
func (a Array) WriteTo(w *bytes.Buffer) error {
	if a == nil {
		_, err := fmt.Fprintf(w, "*-1\r\n")
		return err
	}

	if _, err := fmt.Fprintf(w, "*%d\r\n", len(a)); err != nil {
		return err
	}

	for _, v := range a {
		if err := v.WriteTo(w); err != nil {
			return err
		}
	}

	return nil
}
