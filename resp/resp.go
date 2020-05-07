// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// RESP(REdis Serialization Protocol) is used for the communication between redis client and server.
//  RESP can serialize different data types like integers, strings, arrays. There is also a specific
//  type for errors. Requests are sent from the client to the Redis server as arrays of strings
//  representing the arguments of the command to execute. Redis replies with a command-specific data type.
//
//  RESP is binary-safe and does not require processing of bulk data transferred from one process to
//  another, because it uses prefixed-length to transfer bulk data.
//
// Detail description of RESP can be found at: http://redis.io/topics/protocol
// This package is to parse RESP.
package resp

import (
	"bufio"
)

func Parse(reader *bufio.Reader) (RESPType, Value, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return WrongType, nil, err
	}

	switch prefix {
	case '+': // Simple String
		return parseSimpleString(reader)
	case '-': // Error
		return parseError(reader)
	case ':': // Integer
		return parseInteger(reader)
	case '$': // Bulk String
		return parseBulkString(reader)
	case '*': // Array
		return parseArray(reader)
	default: // Inline Command
		if err := reader.UnreadByte(); err != nil {
			return WrongType, nil, err
		}
		return parseInlineCommand(reader)
	}
}

func readLine(reader *bufio.Reader) ([]byte, error) {
	line := []byte{}
	more := true

	for more {
		buf, isPrefix, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}

		line = append(line, buf...)
		more = isPrefix
	}

	return line, nil
}

func readInt(reader *bufio.Reader) (int64, error) {
	line, err := readLine(reader)
	if err != nil {
		return 0, err
	}

	sign := int64(1)
	if line[0] == '-' {
		sign = -1
		line = line[1:]
	}

	i := int64(0)
	for _, c := range line {
		i = i*10 + int64(c-'0')
	}

	return i * sign, nil
}

func parseSimpleString(reader *bufio.Reader) (RESPType, SimpleString, error) {
	line, err := readLine(reader)
	if err != nil {
		return SimpleStringType, SimpleString(""), err
	}

	return SimpleStringType, SimpleString(line), nil
}

func parseInteger(reader *bufio.Reader) (RESPType, Integer, error) {
	i, err := readInt(reader)
	if err != nil {
		return IntegerType, Integer(0), err
	}

	return IntegerType, Integer(i), nil
}

func parseError(reader *bufio.Reader) (RESPType, Error, error) {
	line, err := readLine(reader)
	if err != nil {
		return ErrorType, Error(""), err
	}

	return ErrorType, Error(line), nil
}

func parseBulkString(reader *bufio.Reader) (RESPType, BulkString, error) {
	i, err := readInt(reader)
	if err != nil {
		return BulkStringType, BulkString(nil), err
	}

	if i == -1 {
		return BulkStringType, BulkString(nil), nil
	}

	b, err := reader.Peek(int(i))
	if err != nil {
		return BulkStringType, BulkString(nil), err
	}
	reader.Discard(int(i) + 2) // +2 for \r\n

	return BulkStringType, BulkString(b), nil
}

func parseArray(reader *bufio.Reader) (RESPType, Array, error) {
	i, err := readInt(reader)
	if err != nil {
		return ArrayType, Array(nil), err
	}

	if i == -1 {
		return ArrayType, Array(nil), nil
	}

	arr := make(Array, i)
	for i := range arr {
		_, v, err := Parse(reader)
		if err != nil {
			return ArrayType, Array(nil), err
		}
		arr[i] = v
	}
	return ArrayType, arr, nil
}

func parseInlineCommand(reader *bufio.Reader) (RESPType, Array, error) {
	line, err := readLine(reader)
	if err != nil {
		return ArrayType, Array(nil), err
	}

	var arr Array
	start := 0
	for i := start; i < len(line); i++ {
		switch line[i] {
		case ' ':
			if i != start {
				arr = append(arr, append(BulkString(nil), line[start:i]...))
				start = i
			}
			start++
		}
	}

	return ArrayType, arr, nil
}
