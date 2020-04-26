package test

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

// help structure, variable and function for rodis testing

var redisPool *redis.Pool
var re redis.Conn

func init() {
	redisPool = &redis.Pool{
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			re, err := redis.Dial("tcp", ":6379")
			if err != nil {
				return nil, err
			}
			/*
				if _, err := re.Do("AUTH", "password"); err != nil {
					re.Close()
					return nil, err
				}
			*/
			return re, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	re = redisPool.Get()
}

type replyType struct {
	vtype string
	value interface{}
}

type rodisTest struct {
	command []interface{}
	reply   replyType
}

func check(r interface{}, v replyType) bool {
	switch v.vtype {
	case "SimpleString":
		if retV, ok := r.(string); ok != true || retV != v.value.(string) {
			return false
		}
	case "Error":
		if retV, ok := r.(error); ok != true || retV.Error() != v.value.(string) {
			return false
		}
	case "Integer":
		if retV, ok := r.(int64); ok != true || retV != v.value.(int64) {
			return false
		}
	case "BulkString":
		if r == nil && v.value == nil {
			return true
		}
		if r == nil && v.value != nil || r != nil && v.value == nil {
			return false
		}
		if retV, ok := r.([]byte); ok != true || string(retV) != string(v.value.([]byte)) {
			return false
		}
	case "Array":
		if r == nil && v.value == nil {
			return true
		}
		if r == nil && v.value != nil || r != nil && v.value == nil {
			return false
		}
		retV, ok := r.([]interface{})
		if !ok {
			return false
		}
		expectV, _ := v.value.([]replyType)
		if len(retV) != len(expectV) {
			return false
		}
		for i := 0; i < len(retV); i++ {
			if !check(retV[i], expectV[i]) {
				return false
			}
		}
	default:
		return false
	}
	return true
}

func runTest(name string, tests []rodisTest, t *testing.T) {
	re.Do("FLUSHDB")
	for i, test := range tests {
		r, _ := re.Do(test.command[0].(string), test.command[1:]...)
		if !check(r, test.reply) {
			t.Errorf("Error %v[%v](%v), Expect: %v,  Get: %#v", name, i, test.command, test.reply, r)
		}
	}
}
