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
	"github.com/rod6/rodis/storage"
)

// Implement for following commands
//
// command		status
// ---------------------
// DEL          done
// EXIST        done
// EXPIRE       done
// EXPIREAT     done
// PEXPIRE      done
// PEXPIREAT    done
// PTTL         done
// TTL          done
// TYPE         done

// del => https://redis.io/commands/del
func del(v resp.CommandArgs, ex *Extras) error {
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
		case storage.String:
			ex.DB.DeleteString(key)
		case storage.Hash:
			ex.DB.DeleteHash(key)
		case storage.List:
			ex.DB.DeleteList(key)
		case storage.Set:
			ex.DB.DeleteHash(key)
		}

		count++
	}
	return resp.Integer(count).WriteTo(ex.Buffer)
}

// exists -> https://redis.io/commands/exists
func exists(v resp.CommandArgs, ex *Extras) error {
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
func expire(v resp.CommandArgs, ex *Extras) error {
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
func expireat(v resp.CommandArgs, ex *Extras) error {
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
func pexpire(v resp.CommandArgs, ex *Extras) error {
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
func pexpireat(v resp.CommandArgs, ex *Extras) error {
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
func pttl(v resp.CommandArgs, ex *Extras) error {
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
func ttl(v resp.CommandArgs, ex *Extras) error {
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
func tipe(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])

	if !exist {
		return resp.SimpleString(storage.TypeString[storage.None]).WriteTo(ex.Buffer)
	}
	return resp.SimpleString(storage.TypeString[tipe]).WriteTo(ex.Buffer)
}
