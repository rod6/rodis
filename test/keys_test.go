package test

import (
	"testing"
)

func TestDel(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"del"}, replyType{"Error", "ERR wrong number of arguments for 'del' command"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"del", "a", "b"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"del", "a", "b", "c"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "b", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "c", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"del", "a", "b", "c", "d"}, replyType{"Integer", int64(2)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", nil}},
		{[]interface{}{"get", "b"}, replyType{"BulkString", nil}},
		{[]interface{}{"get", "c"}, replyType{"BulkString", nil}},
	}
	runTest("DEL", tests, t)
}

func TestExists(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"exists"}, replyType{"Error", "ERR wrong number of arguments for 'exists' command"}},
		{[]interface{}{"exists", "a"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"exists", "a", "b"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"exists", "a", "b", "c"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "b", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "c", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"exists", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"exists", "a", "b", "c", "d"}, replyType{"Integer", int64(3)}},
	}
	runTest("DEL", tests, t)
}

func TestType(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"type"}, replyType{"Error", "ERR wrong number of arguments for 'type' command"}},
		{[]interface{}{"type", "a", "b"}, replyType{"Error", "ERR wrong number of arguments for 'type' command"}},
		{[]interface{}{"type", "a"}, replyType{"SimpleString", "none"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"type", "a"}, replyType{"SimpleString", "string"}},
	}
	runTest("TYPE", tests, t)
}
