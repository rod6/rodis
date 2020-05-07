// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"strconv"

	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

// command
// -------
// AUTH
// ECHO
// FLUSHDB
// PING
// SELECT

// auth: https://redis.io/commands/auth
func auth(v Args, ex *Extras) error {
	if ex.Password == "" {
		return resp.NewError(ErrNoNeedPassword).WriteTo(ex.Buffer)
	}
	if string(v[0]) != ex.Password {
		ex.Authed = false
		return resp.NewError(ErrWrongPassword).WriteTo(ex.Buffer)
	}
	ex.Authed = true
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// echo: https://redis.io/commands/echo
func echo(v Args, ex *Extras) error {
	return resp.BulkString(v[0]).WriteTo(ex.Buffer)
}

// ping: https://redis.io/commands/ping
func ping(v Args, ex *Extras) error {
	return resp.PongSimpleString.WriteTo(ex.Buffer)
}

// flushdb: https://redis.io/commands/flushdb
func flushdb(v Args, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	if err := ex.DB.Flush(); err != nil {
		return err
	}
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// selectdb: https://redis.io/commands/select
func selectdb(v Args, ex *Extras) error {
	s := string(v[0])
	index, err := strconv.Atoi(s)
	if err != nil {
		return resp.NewError(ErrSelectInvalidIndex).WriteTo(ex.Buffer)
	}

	if index < 0 || index > 15 {
		return resp.NewError(ErrSelectInvalidIndex).WriteTo(ex.Buffer)
	}
	ex.DB = storage.Select(index)
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}
