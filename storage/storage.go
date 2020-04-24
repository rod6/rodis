// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb/opt"
)

var storage [16]*LevelDB

func Open(dbPath string, options *opt.Options) error {
	for i := 0; i < 16; i++ {
		d := dbPath + fmt.Sprintf("/%d", i)
		db, err := open(d, options)
		if err != nil {
			return err
		}
		storage[i] = db
	}
	return nil
}

func Select(i int) *LevelDB {
	return storage[i]
}

func Close() {
	for _, ldb := range storage {
		ldb.close()
	}
}
