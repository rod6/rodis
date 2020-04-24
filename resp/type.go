// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package resp

import (
	"bytes"
	"fmt"
)

type RESPType int

const (
	WrongType = iota // wrong input
	SimpleStringType
	ErrorType
	IntegerType
	BulkStringType
	ArrayType
)

type Value interface {
	WriteTo(*bytes.Buffer) error
}

// RESP SimpleString
type SimpleString string

const OkSimpleString = SimpleString("OK")
const PongSimpleString = SimpleString("PONG")

func (s SimpleString) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", s)
	return err
}

func (s SimpleString) String() string {
	return string(s)
}

// RESP Integer
type Integer int64

const (
	ZeroInteger        = Integer(0)
	OneInteger         = Integer(1)
	NegativeOneInteger = Integer(-1)
)

func (i Integer) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, ":%d\r\n", i)
	return err
}

// RESP Error
type Error string

func NewError(format string, v ...interface{}) Error {
	return Error(fmt.Sprintf(format, v...))
}

func (e Error) Error() string {
	return string(e)
}

func (e Error) WriteTo(w *bytes.Buffer) error {
	_, err := fmt.Fprintf(w, "-%s\r\n", e)
	return err
}

// RESP Bulk String
type BulkString []byte

var (
	NilBulkString   = BulkString(nil)
	EmptyBulkString = BulkString([]byte(""))
)

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

// RESP Array
type Array []Value

var EmptyArray = Array{}

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

type CommandArgs []BulkString

func (a Array) ToArgs() CommandArgs {
	c := make(CommandArgs, len(a))
	for i, v := range a {
		c[i] = v.(BulkString)
	}
	return c
}

func (args CommandArgs) ToBytes() [][]byte {
	c := make([][]byte, len(args))
	for i, v := range args {
		c[i] = v
	}
	return c
}
