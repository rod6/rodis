// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"github.com/rod6/rodis/resp"
	"github.com/syndtr/goleveldb/leveldb"
)

// encodeStringKey encodes string type key
func encodeStringKey(key []byte) []byte {
	return append([]byte{ValuePrefix}, key...)
}

// DeleteString deletes string data
func (ldb *LevelDB) DeleteString(key []byte) {
	ldb.delete([][]byte{encodeMetaKey(key), encodeStringKey(key)})
}

// GetString retrieves string data
func (ldb *LevelDB) GetString(key []byte) []byte {
	return ldb.get(encodeStringKey(key))
}

// PutString writes string data to leveldb
func (ldb *LevelDB) PutString(key []byte, value []byte) {
	batch := new(leveldb.Batch)
	batch.Put(encodeMetaKey(key), encodeMetadata(resp.String))
	batch.Put(encodeStringKey(key), value)
	if err := ldb.db.Write(batch, nil); err != nil {
		panic(err)
	}
}
