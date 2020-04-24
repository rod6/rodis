package test

import (
	"testing"
)

func TestHdel(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hdel"}, replyType{"Error", "ERR wrong number of arguments for 'hdel' command"}},
		{[]interface{}{"hdel", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hdel' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hdel", "a", "a1"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a2", "donr"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a3", "rod"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hdel", "a", "a1"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hdel", "a", "b"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"hdel", "a", "a2", "a3", "a4"}, replyType{"Integer", int64(2)}},
	}
	runTest("HDEL", tests, t)
}

func TestHexists(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hexists"}, replyType{"Error", "ERR wrong number of arguments for 'hexists' command"}},
		{[]interface{}{"hexists", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hexists' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hexists", "a", "a1"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hexists", "a", "a1"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hexists", "a", "a2"}, replyType{"Integer", int64(0)}},
	}
	runTest("HEXISTS", tests, t)
}

func TestHget(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hget"}, replyType{"Error", "ERR wrong number of arguments for 'hget' command"}},
		{[]interface{}{"hget", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hget' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hget", "a", "a1"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hget", "a", "a1"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"hget", "a", "a2"}, replyType{"BulkString", nil}},
	}
	runTest("HGET", tests, t)
}

func TestHgetall(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hgetall"}, replyType{"Error", "ERR wrong number of arguments for 'hgetall' command"}},
		{[]interface{}{"hgetall", "a", "a1"}, replyType{"Error", "ERR wrong number of arguments for 'hgetall' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hgetall", "a"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a2", "dongr"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hgetall", "a"}, replyType{"Array", []replyType{replyType{"BulkString", []byte("a1")}, replyType{"BulkString", []byte("foobar")}, replyType{"BulkString", []byte("a2")}, replyType{"BulkString", []byte("dongr")}}}},
		{[]interface{}{"hgetall", "b"}, replyType{"Array", []replyType{}}},
	}
	runTest("HGETALL", tests, t)
}

func TestHincrby(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hincrby"}, replyType{"Error", "ERR wrong number of arguments for 'hincrby' command"}},
		{[]interface{}{"hincrby", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hincrby' command"}},
		{[]interface{}{"hincrby", "a", "a1"}, replyType{"Error", "ERR wrong number of arguments for 'hincrby' command"}},
		{[]interface{}{"hincrby", "a", "a1", "1", "ab"}, replyType{"Error", "ERR wrong number of arguments for 'hincrby' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hincrby", "a", "a1", "1"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hincrby", "a", "a1", "5"}, replyType{"Integer", int64(5)}},
		{[]interface{}{"hincrby", "a", "a1", "5"}, replyType{"Integer", int64(10)}},
		{[]interface{}{"hget", "a", "a1"}, replyType{"BulkString", []byte("10")}},
	}
	runTest("HINCRBY", tests, t)
}

func TestHincrbyfloat(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hincrbyfloat"}, replyType{"Error", "ERR wrong number of arguments for 'hincrbyfloat' command"}},
		{[]interface{}{"hincrbyfloat", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hincrbyfloat' command"}},
		{[]interface{}{"hincrbyfloat", "a", "a1"}, replyType{"Error", "ERR wrong number of arguments for 'hincrbyfloat' command"}},
		{[]interface{}{"hincrbyfloat", "a", "a1", "1", "ab"}, replyType{"Error", "ERR wrong number of arguments for 'hincrbyfloat' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hincrbyfloat", "a", "a1", "1"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hincrbyfloat", "a", "a1", "5.5"}, replyType{"BulkString", []byte("5.5")}},
		{[]interface{}{"hincrbyfloat", "a", "a1", "11.4"}, replyType{"BulkString", []byte("16.9")}},
		{[]interface{}{"hget", "a", "a1"}, replyType{"BulkString", []byte("16.9")}},
	}
	runTest("HINCRBYFLOAT", tests, t)
}

func TestHkeys(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hkeys"}, replyType{"Error", "ERR wrong number of arguments for 'hkeys' command"}},
		{[]interface{}{"hkeys", "a", "a1"}, replyType{"Error", "ERR wrong number of arguments for 'hkeys' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hkeys", "a"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a2", "dongr"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hkeys", "a"}, replyType{"Array", []replyType{replyType{"BulkString", []byte("a1")}, replyType{"BulkString", []byte("a2")}}}},
		{[]interface{}{"hkeys", "b"}, replyType{"Array", []replyType{}}},
	}
	runTest("HKEYS", tests, t)
}

func TestHlen(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hlen"}, replyType{"Error", "ERR wrong number of arguments for 'hlen' command"}},
		{[]interface{}{"hlen", "a", "a1"}, replyType{"Error", "ERR wrong number of arguments for 'hlen' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hlen", "a"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a2", "dongr"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hlen", "a"}, replyType{"Integer", int64(2)}},
		{[]interface{}{"hlen", "b"}, replyType{"Integer", int64(0)}},
	}
	runTest("HLEN", tests, t)
}

func TestHmget(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hmget"}, replyType{"Error", "ERR wrong number of arguments for 'hmget' command"}},
		{[]interface{}{"hmget", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hmget' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hmget", "a", "a1"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a2", "dongr"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hmget", "a", "a1", "a2", "a3"}, replyType{"Array", []replyType{replyType{"BulkString", []byte("foobar")}, replyType{"BulkString", []byte("dongr")}, replyType{"BulkString", nil}}}},
		//{[]interface{}{"hmget", "a", "a1", "a2", "a3"}, replyType{"Array", []replyType{replyType{"BulkString", []byte("foobar")}, replyType{"BulkString", []byte("dongr")}}}},
		{[]interface{}{"hmget", "b", "b1"}, replyType{"Array", []replyType{replyType{"BulkString", nil}}}},
	}
	runTest("HMGET", tests, t)
}

func TestHmset(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hmset"}, replyType{"Error", "ERR wrong number of arguments for 'hmset' command"}},
		{[]interface{}{"hmset", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hmset' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hmset", "a", "a1", "dong"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hmset", "a", "a1", "foobar", "a2"}, replyType{"Error", "ERR wrong number of arguments for 'hmset' command"}},
		{[]interface{}{"hmset", "a", "a1", "foobar", "a2", "dongr"}, replyType{"SimpleString", "OK"}},
	}
	runTest("HMSET", tests, t)
}

func TestHset(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hset"}, replyType{"Error", "ERR wrong number of arguments for 'hset' command"}},
		{[]interface{}{"hset", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hset' command"}},
		{[]interface{}{"hset", "a", "a1"}, replyType{"Error", "ERR wrong number of arguments for 'hset' command"}},
		{[]interface{}{"hset", "a", "a1", "foobar", "a2"}, replyType{"Error", "ERR wrong number of arguments for 'hset' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "dongr"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"hget", "a", "a1"}, replyType{"BulkString", []byte("dongr")}},
	}
	runTest("HSET", tests, t)
}

func TestHsetnx(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hsetnx"}, replyType{"Error", "ERR wrong number of arguments for 'hsetnx' command"}},
		{[]interface{}{"hsetnx", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hsetnx' command"}},
		{[]interface{}{"hsetnx", "a", "a1"}, replyType{"Error", "ERR wrong number of arguments for 'hsetnx' command"}},
		{[]interface{}{"hsetnx", "a", "a1", "foobar", "a2"}, replyType{"Error", "ERR wrong number of arguments for 'hsetnx' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hsetnx", "a", "a1", "foobar"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hsetnx", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hsetnx", "a", "a1", "dongr"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"hget", "a", "a1"}, replyType{"BulkString", []byte("foobar")}},
	}
	runTest("HSETNX", tests, t)
}

func TestHstrlen(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hstrlen"}, replyType{"Error", "ERR wrong number of arguments for 'hstrlen' command"}},
		{[]interface{}{"hstrlen", "a"}, replyType{"Error", "ERR wrong number of arguments for 'hstrlen' command"}},
		{[]interface{}{"hstrlen", "a", "a1", "a2"}, replyType{"Error", "ERR wrong number of arguments for 'hstrlen' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hstrlen", "a", "a1"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hstrlen", "a", "a1"}, replyType{"Integer", int64(6)}},
		{[]interface{}{"hstrlen", "a", "a2"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"hstrlen", "b", "a2"}, replyType{"Integer", int64(0)}},
	}
	runTest("HSTRLEN", tests, t)
}

func TestHvals(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"hvals"}, replyType{"Error", "ERR wrong number of arguments for 'hvals' command"}},
		{[]interface{}{"hvals", "a", "a1"}, replyType{"Error", "ERR wrong number of arguments for 'hvals' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"hvals", "a"}, replyType{"Error", "WRONGTYPE Operation against a key holding the wrong kind of value"}},
		{[]interface{}{"del", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a1", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hset", "a", "a2", "dongr"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"hvals", "a"}, replyType{"Array", []replyType{replyType{"BulkString", []byte("foobar")}, replyType{"BulkString", []byte("dongr")}}}},
		{[]interface{}{"hvals", "b"}, replyType{"Array", []replyType{}}},
	}
	runTest("HVALS", tests, t)
}
