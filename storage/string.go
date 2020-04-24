// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

// DeleteString deletes string data
func (ldb *LevelDB) DeleteString(key []byte) {
	metaKey := encodeMetaKey(key)
	valueKey := encodeStringKey(key)

	ldb.delete([][]byte{metaKey, valueKey})
}

// GetString retrieves string data
func (ldb *LevelDB) GetString(key []byte) []byte {
	valueKey := encodeStringKey(key)
	return ldb.get(valueKey)
}

// PutString writes string data to leveldb
func (ldb *LevelDB) PutString(key []byte, value []byte, expireAt *time.Time) {
	metaKey := encodeMetaKey(key)
	valueKey := encodeStringKey(key)

	exists, tipe, _ := ldb.has(metaKey)
	if exists && tipe != String { // If exists data is not string, should delete it.
		ldb.delete([][]byte{metaKey, valueKey})
	}

	batch := new(leveldb.Batch)
	batch.Put(metaKey, encodeMetadata(String, expireAt))
	batch.Put(valueKey, value)
	if err := ldb.db.Write(batch, nil); err != nil {
		panic(err)
	}
}
