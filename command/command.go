// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/libgo/logx"
	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

type Extras struct {
	DB       *storage.LevelDB
	Buffer   *bytes.Buffer
	Authed   bool
	Password string
}

// command handle function
type commandFunc func(v resp.CommandArgs, ex *Extras) error

// command map attr struct
type attr struct {
	f commandFunc // func for the command
	c int         // arg count for the command
}

// commands, a map type with name as the key
var commands = map[string]*attr{
	// connection
	"auth":    {auth, 2},
	"echo":    {echo, 2},
	"ping":    {ping, 1},
	"command": {ping, 1},
	"select":  {selectdb, 2},

	// server
	"flushdb": {flushdb, 1},

	// keys
	"del":    {del, 0},
	"exists": {exists, 0},
	"type":   {tipe, 2},

	// strings
	"append":      {appendx, 3},
	"bitcount":    {bitcount, 0},
	"bitop":       {bitop, 0},
	"bitpos":      {bitpos, 0},
	"decr":        {decr, 2},
	"decrby":      {decrby, 3},
	"get":         {get, 2},
	"getbit":      {getbit, 3},
	"getrange":    {getrange, 4},
	"getset":      {getset, 3},
	"incr":        {incr, 2},
	"incrby":      {incrby, 3},
	"incrbyfloat": {incrbyfloat, 3},
	"mget":        {mget, 0},
	"mset":        {mset, 0},
	"msetnx":      {msetnx, 0},
	"psetex":      {psetex, 4},
	"set":         {set, 0},
	"setbit":      {setbit, 4},
	"setex":       {setex, 4},
	"setnx":       {setnx, 3},
	"setrange":    {setrange, 4},
	"strlen":      {strlen, 2},

	// hashes
	"hdel":         {hdel, 0},
	"hexists":      {hexists, 3},
	"hget":         {hget, 3},
	"hgetall":      {hgetall, 2},
	"hincrby":      {hincrby, 4},
	"hincrbyfloat": {hincrbyfloat, 4},
	"hkeys":        {hkeys, 2},
	"hlen":         {hlen, 2},
	"hmget":        {hmget, 0},
	"hmset":        {hmset, 0},
	"hset":         {hset, 4},
	"hsetnx":       {hsetnx, 4},
	"hstrlen":      {hstrlen, 3},
	"hvals":        {hvals, 2},
}

// Get command handler
func findCmdFunc(c string) (*attr, error) {
	a, ok := commands[c]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`cannot find command '%s'`, c))
	}
	return a, nil
}

// Handle command
func Handle(v resp.Array, ex *Extras) error {
	ex.Buffer.Truncate(0) // Truncate all data in the buffer

	if len(v) == 0 {
		logx.Debug("Command handler, len of the input array is 0")
		return resp.NewError(ErrFmtNoCommand).WriteTo(ex.Buffer)
	}

	args := v.ToArgs()
	logx.Debugf("Command handling:%v", humanArgs(args))

	cmd := strings.ToLower(args[0].String())
	a, err := findCmdFunc(cmd)
	if err != nil {
		logx.Debugf("Command handler, cannt found command: %v", cmd)
		return resp.NewError(ErrFmtUnknownCommand, cmd).WriteTo(ex.Buffer)
	}

	if a.c != 0 && len(v) != a.c { //a.c = 0 means to check the number in f
		return resp.NewError(ErrFmtWrongNumberArgument, cmd).WriteTo(ex.Buffer)
	}

	if !ex.Authed && ex.Password != "" && cmd != "auth" {
		return resp.NewError(ErrAuthed).WriteTo(ex.Buffer)
	}

	return a.f(args[1:], ex)
}

func humanArgs(args resp.CommandArgs) string {
	s := ""
	for _, a := range args {
		s = s + ` '` + a.String() + `'`
	}
	return s
}

const (
	ErrFmtNoCommand           = `ERR no command`
	ErrFmtUnknownCommand      = `ERR unknown command '%s'`
	ErrWrongType              = `WRONGTYPE Operation against a key holding the wrong kind of value`
	ErrFmtWrongNumberArgument = `ERR wrong number of arguments for '%s' command`
	ErrFmtSyntax              = `ERR syntax error`
	ErrAuthed                 = `NOAUTH Authentication required.`
	ErrWrongPassword          = `ERR invalid password`
	ErrNoNeedPassword         = `ERR Client sent AUTH, but no password is set`
	ErrSelectInvalidIndex     = `ERR invalid DB index`
	ErrNotValidInt            = `ERR value is not an integer or out of range`
	ErrNotValidFloat          = `ERR value is not a valid float`
	ErrBitOPNotError          = `ERR BITOP NOT must be called with a single source key.`
	ErrSyntax                 = `ERR syntax error`
	ErrShouldBe0or1           = `ERR The bit argument must be 1 or 0.`
	ErrBitOffsetInvalid       = `ERR bit offset is not an integer or out of range`
	ErrBitValueInvalid        = `ERR bit is not an integer or out of range`
	ErrStringExccedLimit      = `ERR string exceeds maximum allowed size (512MB)`
	ErrOffsetOutRange         = `ERR offset is out of range`
)
