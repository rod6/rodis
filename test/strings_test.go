package test

import (
	"testing"
)

// string group
func TestAppend(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"append"}, replyType{"Error", "ERR wrong number of arguments for 'append' command"}},
		{[]interface{}{"append", "a"}, replyType{"Error", "ERR wrong number of arguments for 'append' command"}},
		{[]interface{}{"append", "a", "foobar"}, replyType{"Integer", int64(6)}},
		{[]interface{}{"append", "a", "abc"}, replyType{"Integer", int64(9)}},
	}
	runTest("APPEND", tests, t)
}

func TestBitCount(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"bitcount"}, replyType{"Error", "ERR wrong number of arguments for 'bitcount' command"}},
		{[]interface{}{"bitcount", "a"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"bitcount", "a", "0"}, replyType{"Error", "ERR syntax error"}},
		{[]interface{}{"bitcount", "a", "0", "2", "5"}, replyType{"Error", "ERR syntax error"}},
		{[]interface{}{"bitcount", "a"}, replyType{"Integer", int64(26)}},
		{[]interface{}{"bitcount", "a", "0", "4"}, replyType{"Integer", int64(22)}},
		{[]interface{}{"bitcount", "a", "0", "5"}, replyType{"Integer", int64(26)}},
		{[]interface{}{"bitcount", "a", "0", "6"}, replyType{"Integer", int64(26)}},
		{[]interface{}{"bitcount", "a", "0", "7"}, replyType{"Integer", int64(26)}},
		{[]interface{}{"bitcount", "a", "0", "-1"}, replyType{"Integer", int64(26)}},
		{[]interface{}{"bitcount", "a", "0", "-2"}, replyType{"Integer", int64(22)}},
		{[]interface{}{"bitcount", "a", "0", "-5"}, replyType{"Integer", int64(10)}},
		{[]interface{}{"bitcount", "a", "0", "-6"}, replyType{"Integer", int64(4)}},
		{[]interface{}{"bitcount", "a", "0", "-7"}, replyType{"Integer", int64(4)}},
		{[]interface{}{"bitcount", "a", "1", "5"}, replyType{"Integer", int64(22)}},
		{[]interface{}{"bitcount", "a", "2", "5"}, replyType{"Integer", int64(16)}},
		{[]interface{}{"bitcount", "a", "3", "5"}, replyType{"Integer", int64(10)}},
		{[]interface{}{"bitcount", "a", "4", "5"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"bitcount", "a", "5", "5"}, replyType{"Integer", int64(4)}},
		{[]interface{}{"bitcount", "a", "6", "5"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"bitcount", "a", "7", "5"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"bitcount", "a", "-1", "5"}, replyType{"Integer", int64(4)}},
		{[]interface{}{"bitcount", "a", "-2", "5"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"bitcount", "a", "-2", "5"}, replyType{"Integer", int64(7)}},
	}
	runTest("BITCOUNT", tests, t)
}

func TestBitOP(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"bitop"}, replyType{"Error", "ERR wrong number of arguments for 'bitop' command"}},
		{[]interface{}{"bitop", "a"}, replyType{"Error", "ERR wrong number of arguments for 'bitop' command"}},
		{[]interface{}{"bitop", "not", "a"}, replyType{"Error", "ERR wrong number of arguments for 'bitop' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "b", "testing"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "c", "dong"}, replyType{"SimpleString", "OK"}},
		// not
		{[]interface{}{"bitop", "not", "d", "a", "b"}, replyType{"Error", "ERR BITOP NOT must be called with a single source key."}},
		{[]interface{}{"bitop", "not", "d", "a"}, replyType{"Integer", int64(6)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte{0x99, 0x90, 0x90, 0x9d, 0x9e, 0x8d}}},
		{[]interface{}{"bitop", "and", "d", "a"}, replyType{"Integer", int64(6)}},
		// and
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"bitop", "and", "d", "a", "b"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("dec`ab\x00")}},
		{[]interface{}{"bitop", "and", "d", "a", "b", "c"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("deb`\x00\x00\x00")}},
		{[]interface{}{"bitop", "and", "d", "a", "b", "c", "e"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("\x00\x00\x00\x00\x00\x00\x00")}},
		// or
		{[]interface{}{"bitop", "or", "d", "a"}, replyType{"Integer", int64(6)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"bitop", "or", "d", "a", "b"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("vo\x7fvi~g")}},
		{[]interface{}{"bitop", "or", "d", "a", "b", "c"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("vo\x7fwi~g")}},
		{[]interface{}{"bitop", "or", "d", "a", "b", "c", "e"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("vo\x7fwi~g")}},
		// xor
		{[]interface{}{"bitop", "xor", "d", "a"}, replyType{"Integer", int64(6)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"bitop", "xor", "d", "a", "b"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("\x12\n\x1c\x16\b\x1cg")}},
		{[]interface{}{"bitop", "xor", "d", "a", "b", "c"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("verq\b\x1cg")}},
		{[]interface{}{"bitop", "xor", "d", "a", "b", "c", "e"}, replyType{"Integer", int64(7)}},
		{[]interface{}{"get", "d"}, replyType{"BulkString", []byte("verq\b\x1cg")}},
	}
	runTest("BITOP", tests, t)
}

func TestBitpos(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"bitpos"}, replyType{"Error", "ERR wrong number of arguments for 'bitpos' command"}},
		{[]interface{}{"bitpos", "a"}, replyType{"Error", "ERR wrong number of arguments for 'bitpos' command"}},
		{[]interface{}{"bitpos", "a", "2"}, replyType{"Error", "ERR The bit argument must be 1 or 0."}},
		{[]interface{}{"bitpos", "a", "1"}, replyType{"Integer", int64(-1)}},
		{[]interface{}{"bitpos", "a", "1", "2"}, replyType{"Integer", int64(-1)}},
		{[]interface{}{"bitpos", "a", "1", "2", "3"}, replyType{"Integer", int64(-1)}},
		{[]interface{}{"bitpos", "a", "0"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"bitpos", "a", "0", "2"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"bitpos", "a", "0", "2", "3"}, replyType{"Integer", int64(0)}},

		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"bitpos", "a", "1"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"bitpos", "a", "1", "2"}, replyType{"Integer", int64(17)}},
		{[]interface{}{"bitpos", "a", "1", "2", "3"}, replyType{"Integer", int64(17)}},
		{[]interface{}{"bitpos", "a", "1", "2", "-1"}, replyType{"Integer", int64(17)}},
		{[]interface{}{"bitpos", "a", "1", "2", "1"}, replyType{"Integer", int64(-1)}},
		{[]interface{}{"bitpos", "a", "0"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"bitpos", "a", "0", "2"}, replyType{"Integer", int64(16)}},
		{[]interface{}{"bitpos", "a", "0", "2", "3"}, replyType{"Integer", int64(16)}},
		{[]interface{}{"bitpos", "a", "0", "2", "-1"}, replyType{"Integer", int64(16)}},
		{[]interface{}{"bitpos", "a", "0", "2", "1"}, replyType{"Integer", int64(-1)}},

		{[]interface{}{"set", "a", "\xff\xf0\x00"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"bitpos", "a", "0"}, replyType{"Integer", int64(12)}},
		{[]interface{}{"set", "a", "\x00\xff\xf0"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"bitpos", "a", "1", "0"}, replyType{"Integer", int64(8)}},
		{[]interface{}{"bitpos", "a", "1", "2"}, replyType{"Integer", int64(16)}},
		{[]interface{}{"set", "a", "\x00\x00\x00"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"bitpos", "a", "1"}, replyType{"Integer", int64(-1)}},
	}
	runTest("BITPOS", tests, t)
}

func TestDecr(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"decr"}, replyType{"Error", "ERR wrong number of arguments for 'decr' command"}},
		{[]interface{}{"decr", "a", "b"}, replyType{"Error", "ERR wrong number of arguments for 'decr' command"}},
		{[]interface{}{"decr", "a"}, replyType{"Integer", int64(-1)}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"decr", "a"}, replyType{"Error", "ERR value is not an integer or out of range"}},
		{[]interface{}{"set", "a", "2"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"decr", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"decr", "a"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"decr", "a"}, replyType{"Integer", int64(-1)}},
		{[]interface{}{"decr", "a"}, replyType{"Integer", int64(-2)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("-2")}},
	}
	runTest("DECR", tests, t)
}

func TestDecrby(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"decrby"}, replyType{"Error", "ERR wrong number of arguments for 'decrby' command"}},
		{[]interface{}{"decrby", "a"}, replyType{"Error", "ERR wrong number of arguments for 'decrby' command"}},
		{[]interface{}{"decrby", "a", "1", "2"}, replyType{"Error", "ERR wrong number of arguments for 'decrby' command"}},
		{[]interface{}{"decrby", "a", "b"}, replyType{"Error", "ERR value is not an integer or out of range"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"decrby", "a", "100"}, replyType{"Error", "ERR value is not an integer or out of range"}},
		{[]interface{}{"set", "a", "200"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"decrby", "a", "100"}, replyType{"Integer", int64(100)}},
		{[]interface{}{"decrby", "a", "100"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"decrby", "a", "100"}, replyType{"Integer", int64(-100)}},
		{[]interface{}{"decrby", "a", "100"}, replyType{"Integer", int64(-200)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("-200")}},
	}
	runTest("DECRBY", tests, t)
}

func TestGet(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"get"}, replyType{"Error", "ERR wrong number of arguments for 'get' command"}},
		{[]interface{}{"get", "a", "b"}, replyType{"Error", "ERR wrong number of arguments for 'get' command"}},

		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"get", "b"}, replyType{"BulkString", nil}},
	}
	runTest("GET", tests, t)
}

func TestGetBit(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"getbit"}, replyType{"Error", "ERR wrong number of arguments for 'getbit' command"}},
		{[]interface{}{"getbit", "a"}, replyType{"Error", "ERR wrong number of arguments for 'getbit' command"}},
		{[]interface{}{"getbit", "a", "0"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"getbit", "a", "100"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"getbit", "a", "10"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"getbit", "a", "11"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"getbit", "a", "12"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"getbit", "a", "13"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"getbit", "a", "14"}, replyType{"Integer", int64(1)}},
	}
	runTest("GETBIT", tests, t)
}

func TestGetRange(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"getrange"}, replyType{"Error", "ERR wrong number of arguments for 'getrange' command"}},
		{[]interface{}{"getrange", "a"}, replyType{"Error", "ERR wrong number of arguments for 'getrange' command"}},
		{[]interface{}{"getrange", "a", "1"}, replyType{"Error", "ERR wrong number of arguments for 'getrange' command"}},
		{[]interface{}{"getrange", "a", "1", "c"}, replyType{"Error", "ERR value is not an integer or out of range"}},

		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"getrange", "a", "1"}, replyType{"Error", "ERR wrong number of arguments for 'getrange' command"}},
		{[]interface{}{"getrange", "a", "1", "c"}, replyType{"Error", "ERR value is not an integer or out of range"}},

		{[]interface{}{"getrange", "a", "0", "0"}, replyType{"BulkString", []byte("f")}},
		{[]interface{}{"getrange", "a", "0", "1"}, replyType{"BulkString", []byte("fo")}},
		{[]interface{}{"getrange", "a", "0", "2"}, replyType{"BulkString", []byte("foo")}},
		{[]interface{}{"getrange", "a", "0", "3"}, replyType{"BulkString", []byte("foob")}},
		{[]interface{}{"getrange", "a", "0", "4"}, replyType{"BulkString", []byte("fooba")}},
		{[]interface{}{"getrange", "a", "0", "5"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"getrange", "a", "0", "6"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"getrange", "a", "0", "7"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"getrange", "a", "0", "-1"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"getrange", "a", "0", "-2"}, replyType{"BulkString", []byte("fooba")}},
		{[]interface{}{"getrange", "a", "0", "-3"}, replyType{"BulkString", []byte("foob")}},
		{[]interface{}{"getrange", "a", "0", "-4"}, replyType{"BulkString", []byte("foo")}},
		{[]interface{}{"getrange", "a", "0", "-5"}, replyType{"BulkString", []byte("fo")}},
		{[]interface{}{"getrange", "a", "0", "-6"}, replyType{"BulkString", []byte("f")}},
		{[]interface{}{"getrange", "a", "0", "-7"}, replyType{"BulkString", []byte("f")}},
		{[]interface{}{"getrange", "a", "0", "-8"}, replyType{"BulkString", []byte("f")}},

		{[]interface{}{"getrange", "a", "-1", "0"}, replyType{"BulkString", []byte("")}},
		{[]interface{}{"getrange", "a", "-5", "0"}, replyType{"BulkString", []byte("")}},
		{[]interface{}{"getrange", "a", "-6", "0"}, replyType{"BulkString", []byte("f")}},
		{[]interface{}{"getrange", "a", "-7", "0"}, replyType{"BulkString", []byte("f")}},

		{[]interface{}{"getrange", "a", "7", "5"}, replyType{"BulkString", []byte("")}},
		{[]interface{}{"getrange", "a", "6", "5"}, replyType{"BulkString", []byte("")}},
		{[]interface{}{"getrange", "a", "5", "5"}, replyType{"BulkString", []byte("r")}},
		{[]interface{}{"getrange", "a", "4", "5"}, replyType{"BulkString", []byte("ar")}},
		{[]interface{}{"getrange", "a", "3", "5"}, replyType{"BulkString", []byte("bar")}},
		{[]interface{}{"getrange", "a", "2", "5"}, replyType{"BulkString", []byte("obar")}},
		{[]interface{}{"getrange", "a", "1", "5"}, replyType{"BulkString", []byte("oobar")}},
		{[]interface{}{"getrange", "a", "0", "5"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"getrange", "a", "-1", "5"}, replyType{"BulkString", []byte("r")}},
		{[]interface{}{"getrange", "a", "-2", "5"}, replyType{"BulkString", []byte("ar")}},
		{[]interface{}{"getrange", "a", "-3", "5"}, replyType{"BulkString", []byte("bar")}},
		{[]interface{}{"getrange", "a", "-4", "5"}, replyType{"BulkString", []byte("obar")}},
		{[]interface{}{"getrange", "a", "-5", "5"}, replyType{"BulkString", []byte("oobar")}},
		{[]interface{}{"getrange", "a", "-6", "5"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"getrange", "a", "-7", "5"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"getrange", "a", "-8", "5"}, replyType{"BulkString", []byte("foobar")}},

		{[]interface{}{"getrange", "a", "-3", "0"}, replyType{"BulkString", []byte("")}},
		{[]interface{}{"getrange", "a", "-3", "-1"}, replyType{"BulkString", []byte("bar")}},
		{[]interface{}{"getrange", "a", "-3", "-2"}, replyType{"BulkString", []byte("ba")}},
		{[]interface{}{"getrange", "a", "-3", "-3"}, replyType{"BulkString", []byte("b")}},
		{[]interface{}{"getrange", "a", "-3", "-4"}, replyType{"BulkString", []byte("")}},
	}
	runTest("GETRANGE", tests, t)
}

func TestGetSet(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"getset"}, replyType{"Error", "ERR wrong number of arguments for 'getset' command"}},
		{[]interface{}{"getset", "a"}, replyType{"Error", "ERR wrong number of arguments for 'getset' command"}},
		{[]interface{}{"getset", "a", "foobar"}, replyType{"BulkString", nil}},
		{[]interface{}{"getset", "a", "dong"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("dong")}},
	}
	runTest("GETSET", tests, t)
}

func TestIncr(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"incr"}, replyType{"Error", "ERR wrong number of arguments for 'incr' command"}},
		{[]interface{}{"incr", "a", "b"}, replyType{"Error", "ERR wrong number of arguments for 'incr' command"}},
		{[]interface{}{"incr", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"incr", "a"}, replyType{"Error", "ERR value is not an integer or out of range"}},
		{[]interface{}{"set", "a", "-2"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"incr", "a"}, replyType{"Integer", int64(-1)}},
		{[]interface{}{"incr", "a"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"incr", "a"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"incr", "a"}, replyType{"Integer", int64(2)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("2")}},
	}
	runTest("INCR", tests, t)
}

func TestIncrby(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"incrby"}, replyType{"Error", "ERR wrong number of arguments for 'incrby' command"}},
		{[]interface{}{"incrby", "a"}, replyType{"Error", "ERR wrong number of arguments for 'incrby' command"}},
		{[]interface{}{"incrby", "a", "1", "2"}, replyType{"Error", "ERR wrong number of arguments for 'incrby' command"}},
		{[]interface{}{"incrby", "a", "b"}, replyType{"Error", "ERR value is not an integer or out of range"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"incrby", "a", "100"}, replyType{"Error", "ERR value is not an integer or out of range"}},
		{[]interface{}{"set", "a", "-200"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"incrby", "a", "100"}, replyType{"Integer", int64(-100)}},
		{[]interface{}{"incrby", "a", "100"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"incrby", "a", "100"}, replyType{"Integer", int64(100)}},
		{[]interface{}{"incrby", "a", "100"}, replyType{"Integer", int64(200)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("200")}},
	}
	runTest("INCRBY", tests, t)
}

func TestIncrbyfloat(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"incrbyfloat"}, replyType{"Error", "ERR wrong number of arguments for 'incrbyfloat' command"}},
		{[]interface{}{"incrbyfloat", "a"}, replyType{"Error", "ERR wrong number of arguments for 'incrbyfloat' command"}},
		{[]interface{}{"incrbyfloat", "a", "1", "2"}, replyType{"Error", "ERR wrong number of arguments for 'incrbyfloat' command"}},
		{[]interface{}{"incrbyfloat", "a", "b"}, replyType{"Error", "ERR value is not a valid float"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"incrbyfloat", "a", "100"}, replyType{"Error", "ERR value is not a valid float"}},
		{[]interface{}{"set", "a", "10.2"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"incrbyfloat", "a", "10.2"}, replyType{"BulkString", []byte("20.4")}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("20.4")}},
	}
	runTest("INCRBYFLOAT", tests, t)
}

func TestMget(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"mget"}, replyType{"Error", "ERR wrong number of arguments for 'mget' command"}},
		{[]interface{}{"mget", "a"}, replyType{"Array", []replyType{replyType{"BulkString", nil}}}},
		{[]interface{}{"mget", "a", "b"}, replyType{"Array", []replyType{replyType{"BulkString", nil}, replyType{"BulkString", nil}}}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "b", "dong"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"mget", "a", "b"}, replyType{"Array", []replyType{replyType{"BulkString", []byte("foobar")}, replyType{"BulkString", []byte("dong")}}}},
		{[]interface{}{"mget", "a", "b", "c"}, replyType{"Array", []replyType{replyType{"BulkString", []byte("foobar")}, replyType{"BulkString", []byte("dong")}, replyType{"BulkString", nil}}}},
		{[]interface{}{"mget", "a", "b", "c", "a"}, replyType{"Array", []replyType{replyType{"BulkString", []byte("foobar")}, replyType{"BulkString", []byte("dong")}, replyType{"BulkString", nil}, replyType{"BulkString", []byte("foobar")}}}},
	}
	runTest("MGET", tests, t)
}

func TestMset(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"mset"}, replyType{"Error", "ERR wrong number of arguments for 'mset' command"}},
		{[]interface{}{"mset", "a"}, replyType{"Error", "ERR wrong number of arguments for 'mset' command"}},
		{[]interface{}{"mset", "a", "1", "b"}, replyType{"Error", "ERR wrong number of arguments for 'mset' command"}},
		{[]interface{}{"mset", "a", "1", "b", "2", "c"}, replyType{"Error", "ERR wrong number of arguments for 'mset' command"}},
		{[]interface{}{"mset", "a", "foobar", "b", "dong", "c", "rodaier"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"get", "b"}, replyType{"BulkString", []byte("dong")}},
		{[]interface{}{"get", "c"}, replyType{"BulkString", []byte("rodaier")}},
	}
	runTest("MSET", tests, t)
}

func TestMsetnx(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"msetnx"}, replyType{"Error", "ERR wrong number of arguments for 'msetnx' command"}},
		{[]interface{}{"msetnx", "a"}, replyType{"Error", "ERR wrong number of arguments for 'msetnx' command"}},
		{[]interface{}{"msetnx", "a", "1", "b"}, replyType{"Error", "ERR wrong number of arguments for 'msetnx' command"}},
		{[]interface{}{"msetnx", "a", "1", "b", "2"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"msetnx", "b", "3", "c", "4"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("1")}},
		{[]interface{}{"get", "b"}, replyType{"BulkString", []byte("2")}},
		{[]interface{}{"get", "c"}, replyType{"BulkString", nil}},
	}
	runTest("MSETNX", tests, t)
}

func TestSet(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"set"}, replyType{"Error", "ERR wrong number of arguments for 'set' command"}},
		{[]interface{}{"set", "a"}, replyType{"Error", "ERR wrong number of arguments for 'set' command"}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"set", "a", "dong", "a"}, replyType{"Error", "ERR syntax error"}},
		{[]interface{}{"set", "a", "dong", "nx"}, replyType{"BulkString", nil}},
		{[]interface{}{"set", "a", "dong", "nx", "xx"}, replyType{"Error", "ERR syntax error"}},
		{[]interface{}{"set", "a", "dong", "xx"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "a", "dong", "xx", "nx"}, replyType{"Error", "ERR syntax error"}},
		{[]interface{}{"set", "a", "dong", "ex"}, replyType{"Error", "ERR syntax error"}},
		{[]interface{}{"set", "a", "dong", "ex", "nx"}, replyType{"Error", "ERR value is not an integer or out of range"}},
		{[]interface{}{"set", "a", "dong", "px"}, replyType{"Error", "ERR syntax error"}},
		{[]interface{}{"set", "a", "dong", "px", "nx"}, replyType{"Error", "ERR value is not an integer or out of range"}},
		{[]interface{}{"set", "a", "dong", "ex", "1"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "a", "dong", "px", "1"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "a", "dong", "px", "1", "px", "100"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "a", "dong", "px", "1", "ex", "1000"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"set", "a", "foobar", "px", "1", "ex", "1000", "xx"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"set", "b", "foobar", "px", "1", "ex", "1000", "nx"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"get", "b"}, replyType{"BulkString", []byte("foobar")}},
	}
	runTest("SET", tests, t)
}

func TestSetbit(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"setbit"}, replyType{"Error", "ERR wrong number of arguments for 'setbit' command"}},
		{[]interface{}{"setbit", "a"}, replyType{"Error", "ERR wrong number of arguments for 'setbit' command"}},
		{[]interface{}{"setbit", "a", "100"}, replyType{"Error", "ERR wrong number of arguments for 'setbit' command"}},
		{[]interface{}{"setbit", "a", "100", "1", "2"}, replyType{"Error", "ERR wrong number of arguments for 'setbit' command"}},
		{[]interface{}{"setbit", "a", "100", "1"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"setbit", "a", "100", "0"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"setbit", "a", "110", "1"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02")}},
	}
	runTest("SETBIT", tests, t)
}

func TestSetnx(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"setnx"}, replyType{"Error", "ERR wrong number of arguments for 'setnx' command"}},
		{[]interface{}{"setnx", "a"}, replyType{"Error", "ERR wrong number of arguments for 'setnx' command"}},
		{[]interface{}{"setnx", "a", "foobar", "b"}, replyType{"Error", "ERR wrong number of arguments for 'setnx' command"}},
		{[]interface{}{"setnx", "a", "foobar"}, replyType{"Integer", int64(1)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("foobar")}},
		{[]interface{}{"setnx", "a", "dong"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("foobar")}},
	}
	runTest("SETNX", tests, t)
}

func TestSetrange(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"setrange"}, replyType{"Error", "ERR wrong number of arguments for 'setrange' command"}},
		{[]interface{}{"setrange", "a"}, replyType{"Error", "ERR wrong number of arguments for 'setrange' command"}},
		{[]interface{}{"setrange", "a", "10"}, replyType{"Error", "ERR wrong number of arguments for 'setrange' command"}},
		{[]interface{}{"setrange", "a", "10", "ab", "de"}, replyType{"Error", "ERR wrong number of arguments for 'setrange' command"}},
		{[]interface{}{"setrange", "a", "536870910", "abc"}, replyType{"Error", "ERR string exceeds maximum allowed size (512MB)"}},
		{[]interface{}{"setrange", "a", "-10", "abc"}, replyType{"Error", "ERR offset is out of range"}},
		{[]interface{}{"setrange", "a", "10", "abc"}, replyType{"Integer", int64(13)}},
		{[]interface{}{"get", "a"}, replyType{"BulkString", []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00abc")}},
		//{[]interface{}{"setrange", "a", "536870910", "ab"}, replyType{"Integer", int64(536870912)}},
	}
	runTest("SETRANGE", tests, t)
}

func TestStrlen(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"strlen"}, replyType{"Error", "ERR wrong number of arguments for 'strlen' command"}},
		{[]interface{}{"strlen", "a", "b"}, replyType{"Error", "ERR wrong number of arguments for 'strlen' command"}},
		{[]interface{}{"strlen", "a"}, replyType{"Integer", int64(0)}},
		{[]interface{}{"set", "a", "foobar"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"strlen", "a"}, replyType{"Integer", int64(6)}},
		{[]interface{}{"set", "a", ""}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"strlen", "a"}, replyType{"Integer", int64(0)}},
	}
	runTest("STRLEN", tests, t)
}
