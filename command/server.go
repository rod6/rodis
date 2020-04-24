// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"github.com/rod6/rodis/resp"
)

func flushdb(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	if err := ex.DB.Flush(); err != nil {
		return err
	}
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}
