// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"strconv"
	"time"

	"github.com/rod6/rodis/resp"
)

// command
// --------
// DEL
// EXIST
// EXPIRE
// EXPIREAT
// PEXPIRE
// PEXPIREAT
// PTTL
// TTL
// TYPE

// del -> https://redis.io/commands/del
func del(v args, ex *Extras) error {
	if len(v) == 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "del").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	count := 0
	for _, key := range v {
		exist, tipe := ex.DB.Has(key)
		if !exist {
			continue
		}
		switch tipe {
		case resp.String:
			ex.DB.DeleteString(key)
		case resp.Hash:
			ex.DB.DeleteHash(key)
		case resp.List:
			ex.DB.DeleteList(key)
		case resp.Set:
			ex.DB.DeleteHash(key)
		case resp.SortedSet:
			ex.DB.DeleteSkip(key)
		}

		count++
	}
	return resp.Integer(count).WriteTo(ex.Buffer)
}

// exists -> https://redis.io/commands/exists
func exists(v args, ex *Extras) error {
	if len(v) == 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "exists").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	count := 0
	for _, key := range v {
		exist, _ := ex.DB.Has(key)
		if !exist {
			continue
		}
		count++
	}
	return resp.Integer(count).WriteTo(ex.Buffer)
}

// expire -> https://redis.io/commands/expire
func expire(v args, ex *Extras) error {
	expire, err := strconv.ParseInt(string(v[1]), 10, 32)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, _ := ex.DB.Has(v[0])

	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	at := time.Now().Add(time.Duration(expire) * time.Second)
	ex.DB.SetExpireAt(v[0], &at)

	return resp.OneInteger.WriteTo(ex.Buffer)
}

// expireat -> https://redis.io/commands/expireat
func expireat(v args, ex *Extras) error {
	expireat, err := strconv.ParseInt(string(v[1]), 10, 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, _ := ex.DB.Has(v[0])

	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	at := time.Unix(expireat, 0)
	ex.DB.SetExpireAt(v[0], &at)

	return resp.OneInteger.WriteTo(ex.Buffer)
}

// pexpire -> https://redis.io/commands/pexpire
func pexpire(v args, ex *Extras) error {
	pexpire, err := strconv.ParseInt(string(v[1]), 10, 32)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, _ := ex.DB.Has(v[0])

	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	at := time.Now().Add(time.Duration(pexpire) * time.Millisecond)
	ex.DB.SetExpireAt(v[0], &at)

	return resp.OneInteger.WriteTo(ex.Buffer)
}

// pexpireat -> https://redis.io/commands/expireat
func pexpireat(v args, ex *Extras) error {
	pexpireat, err := strconv.ParseInt(string(v[1]), 10, 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, _ := ex.DB.Has(v[0])

	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	at := time.Unix(0, pexpireat)
	ex.DB.SetExpireAt(v[0], &at)

	return resp.OneInteger.WriteTo(ex.Buffer)
}

// pttl -> https://redis.io/commands/pttl
func pttl(v args, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, _ := ex.DB.Has(v[0])

	if !exist {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}

	at := ex.DB.GetExpireAt(v[0])
	if at == nil {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}

	duration := at.Sub(time.Now())
	ttl := duration / time.Millisecond
	return resp.Integer(ttl).WriteTo(ex.Buffer)
}

// ttl -> https://redis.io/commands/ttl
func ttl(v args, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, _ := ex.DB.Has(v[0])

	if !exist {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}

	at := ex.DB.GetExpireAt(v[0])
	if at == nil {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}

	duration := at.Sub(time.Now())
	ttl := duration / time.Second
	return resp.Integer(ttl).WriteTo(ex.Buffer)
}

// tipe -> https://redis.io/commands/type
func tipe(v args, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])

	if !exist {
		return resp.SimpleString(resp.TypeString[resp.None]).WriteTo(ex.Buffer)
	}
	return resp.SimpleString(resp.TypeString[tipe]).WriteTo(ex.Buffer)
}
