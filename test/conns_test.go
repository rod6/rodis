package test

import (
	"testing"
)

// connection group
func TestPing(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"ping", "1"}, replyType{"Error", "ERR wrong number of arguments for 'ping' command"}},
		{[]interface{}{"ping"}, replyType{"SimpleString", "PONG"}},
	}
	runTest("PING", tests, t)
}

func TestEcho(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"echo"}, replyType{"Error", "ERR wrong number of arguments for 'echo' command"}},
		{[]interface{}{"echo", "test"}, replyType{"BulkString", []byte("test")}},
	}
	runTest("ECHO", tests, t)
}

func TestSelect(t *testing.T) {
	tests := []rodisTest{
		{[]interface{}{"select"}, replyType{"Error", "ERR wrong number of arguments for 'select' command"}},
		{[]interface{}{"select", "a"}, replyType{"Error", "ERR invalid DB index"}},
		{[]interface{}{"select", "16"}, replyType{"Error", "ERR invalid DB index"}},
		{[]interface{}{"select", "-1"}, replyType{"Error", "ERR invalid DB index"}},
		{[]interface{}{"select", "15"}, replyType{"SimpleString", "OK"}},
		{[]interface{}{"select", "0"}, replyType{"SimpleString", "OK"}},
	}
	runTest("SELECT", tests, t)
}
