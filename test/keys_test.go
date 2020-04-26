package test

import (
	"strconv"
	"testing"
	"time"
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
	runTest("EXISTS", tests, t)
}

func TestExpire(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"expire", "a"}, replyType{"Error", "ERR wrong number of arguments for 'expire' command"}},
		{[]interface{}{"expire", "a", "10"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"expire", "a", "a"}, replyType{"Error", "ERR value is not an integer or out of range"}},
	}
	runTest("EXPIRE", tests, t)
}

func TestExpireat(t *testing.T) {
	at := time.Now().Add(10 * time.Second).Unix()
	tests := []rodisTest{
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"expireat", "a"}, replyType{"Error", "ERR wrong number of arguments for 'expireat' command"}},
		{[]interface{}{"expireat", "a", strconv.FormatInt(at, 10)}, replyType{"Integer", int64(1)}},
		{[]interface{}{"expireat", "a", "a"}, replyType{"Error", "ERR value is not an integer or out of range"}},
	}
	runTest("EXPIREAT", tests, t)
}

func TestPexpire(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"pexpire", "a"}, replyType{"Error", "ERR wrong number of arguments for 'pexpire' command"}},
		{[]interface{}{"pexpire", "a", "1000"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"pexpire", "a", "a"}, replyType{"Error", "ERR value is not an integer or out of range"}},
	}
	runTest("PEXPIRE", tests, t)
}

func TestPexpireat(t *testing.T) {
	// TODO
}

func TestPttl(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"pexpire", "a", "1000"}, replyType{"Integer", int64(1)}},
		// {[]interface{}{"pttl", "a"}, replyType{"Integer", int64(999)}},
		{[]interface{}{"pttl", "a", "b"}, replyType{"Error", "ERR wrong number of arguments for 'pttl' command"}},
	}
	runTest("PTTL", tests, t)
}

func TestTtl(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"expire", "a", "10"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"ttl", "a", "b"}, replyType{"Error", "ERR wrong number of arguments for 'ttl' command"}},
		{[]interface{}{"ttl", "a"}, replyType{"Integer", int64(9)}},
	}
	runTest("PTTL", tests, t)
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
