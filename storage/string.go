// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"github.com/syndtr/goleveldb/leveldb"
)

// encodeStringKey encodes string type key
func encodeStringKey(key []byte) []byte {
	valueKey := make([]byte, 1 /* '+' */ +len(key))
	valueKey[0] = ValuePrefix
	copy(valueKey[1:], key)
	return valueKey
}

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
func (ldb *LevelDB) PutString(key []byte, value []byte) {
	metaKey := encodeMetaKey(key)
	valueKey := encodeStringKey(key)

	exists, tipe := ldb.has(metaKey)
	if exists && tipe != String { // If exists data is not string, should delete it.
		ldb.delete([][]byte{metaKey, valueKey})
	}

	batch := new(leveldb.Batch)
	batch.Put(metaKey, encodeMetadata(String))
	batch.Put(valueKey, value)
	if err := ldb.db.Write(batch, nil); err != nil {
		panic(err)
	}
}
