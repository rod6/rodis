// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

// del => https://redis.io/commands/del
func del(v resp.CommandArgs, ex *Extras) error {
	if len(v) == 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "del").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	count := 0
	for _, key := range v {
		exists, tipe, _ := ex.DB.Has(key)
		if !exists {
			continue
		}
		switch tipe {
		case storage.String:
			ex.DB.DeleteString(key)
		case storage.Hash:
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
		exists, _, _ := ex.DB.Has(key)
		if !exists {
			continue
		}
		count++
	}
	return resp.Integer(count).WriteTo(ex.Buffer)
}

func tipe(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exists, tipe, _ := ex.DB.Has(v[0])

	if !exists {
		return resp.SimpleString(storage.TypeString[storage.None]).WriteTo(ex.Buffer)
	}
	return resp.SimpleString(storage.TypeString[tipe]).WriteTo(ex.Buffer)
}
