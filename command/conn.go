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
func auth(v args, ex *Extras) error {
	if ex.Password == "" {
		return resp.NewError(ErrNoNeedPassword).WriteTo(ex.Buffer)
	}
	if v[0].String() != ex.Password {
		ex.Authed = false
		return resp.NewError(ErrWrongPassword).WriteTo(ex.Buffer)
	}
	ex.Authed = true
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// echo: https://redis.io/commands/echo
func echo(v args, ex *Extras) error {
	return v[0].WriteTo(ex.Buffer)
}

// ping: https://redis.io/commands/ping
func ping(v args, ex *Extras) error {
	return resp.PongSimpleString.WriteTo(ex.Buffer)
}

// flushdb: https://redis.io/commands/flushdb
func flushdb(v args, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	if err := ex.DB.Flush(); err != nil {
		return err
	}
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// selectdb: https://redis.io/commands/select
func selectdb(v args, ex *Extras) error {
	s := v[0].String()
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
