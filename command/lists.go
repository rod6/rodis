// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"strconv"
	"strings"

	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

// Implement for command list in http://redis.io/commands#list
//
// command		status
// ------------------------
// LINDEX       done
// LLEN         done
// LPOP         done
// LPUSH        done
// LPUSHX       done
// LRANGE       done
// RPOP         done
// RPUSH        done
// RPUSHX       done

// lindex -> https://redis.io/commands/lindex
func lindex(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	index, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	l := ex.DB.GetListLength(v[0])
	if index > int(l)-1 || index < (-1)*int(l) {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}

	val := []byte{}
	if index >= 0 {
		val = ex.DB.GetLindexFromHead(v[0], uint32(index))
	} else {
		val = ex.DB.GetLindexFromTail(v[0], uint32(-1*index-1))
	}
	return resp.BulkString(val).WriteTo(ex.Buffer)
}

// linsert -> https://redis.io/commands/linsert
func linsert(v resp.CommandArgs, ex *Extras) error {
	d := strings.ToLower(string(v[1]))
	if d != "before" && d != "after" {
		return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
	}

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	l := ex.DB.InsertList(v[0], d, v[2], v[3])
	return resp.Integer(l).WriteTo(ex.Buffer)
}

// llen -> https://redis.io/commands/llen
func llen(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	l := ex.DB.GetListLength(v[0])
	return resp.Integer(l).WriteTo(ex.Buffer)
}

// lpop -> https://redis.io/commands/lpop
func lpop(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	val := ex.DB.PopListHead(v[0])
	if len(val) == 0 {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}

	return resp.BulkString(val).WriteTo(ex.Buffer)
}

// lpush -> https://redis.io/commands/lpush
func lpush(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "lpush").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	var length uint32
	for _, val := range v[1:] {
		length = ex.DB.PushListHead(v[0], storage.List, val)
	}
	return resp.Integer(length).WriteTo(ex.Buffer)
}

// lpushx -> https://redis.io/commands/lpushx
func lpushx(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "lpushx").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	var length uint32
	for _, val := range v[1:] {
		length = ex.DB.PushListHead(v[0], storage.List, val)
	}
	return resp.Integer(length).WriteTo(ex.Buffer)
}

// rpop -> https://redis.io/commands/rpop
func rpop(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	val := ex.DB.PopListTail(v[0])
	if len(val) == 0 {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}

	return resp.BulkString(val).WriteTo(ex.Buffer)
}

// rpush -> https://redis.io/commands/rpush
func rpush(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "rpush").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	var length uint32
	for _, val := range v[1:] {
		length = ex.DB.PushListTail(v[0], storage.List, val)
	}
	return resp.Integer(length).WriteTo(ex.Buffer)
}

// rpushx -> https://redis.io/commands/rpushx
func rpushx(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "rpush").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	var length uint32
	for _, val := range v[1:] {
		length = ex.DB.PushListTail(v[0], storage.List, val)
	}
	return resp.Integer(length).WriteTo(ex.Buffer)
}

// lrange -> https://redis.io/commands/lrange
func lrange(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	start, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	end, err := strconv.Atoi(string(v[2]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	arr := resp.Array{}
	elements := ex.DB.GetListRange(v[0], start, end)

	for _, element := range elements {
		arr = append(arr, resp.BulkString(element))
	}
	return arr.WriteTo(ex.Buffer)
}

// lset -> https://redis.io/commands/lset
func lset(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.NewError(ErrNoSuchKey).WriteTo(ex.Buffer)
	}
	if exist && tipe != storage.List {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	index, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	err = ex.DB.SetListElement(v[0], index, v[2])
	if err != nil {
		resp.NewError(ErrIndexOutRange).WriteTo(ex.Buffer)
	}
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}
