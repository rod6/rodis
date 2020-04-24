// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// DeleteHash deletes all hash data
func (ldb *LevelDB) DeleteHash(key []byte) {
	keys := [][]byte{encodeMetaKey(key)}

	// enum fields, and delete all
	hashPrefix := encodeHashFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	for iter.Next() {
		key := append([]byte{}, iter.Key()...)
		keys = append(keys, key)
	}
	iter.Release()
	ldb.delete(keys)
}

// DeleteHashFields deletes hash fields
func (ldb *LevelDB) DeleteHashFields(key []byte, fields [][]byte) {
	// Delete fields
	keys := make([][]byte, len(fields))
	for i, field := range fields {
		keys[i] = encodeHashFieldKey(key, field)
	}
	ldb.delete(keys)

	// After delete, remove the hash meta entry if no fields in this hash
	hashPrefix := encodeHashFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	if !iter.Next() {
		ldb.delete([][]byte{encodeMetaKey(key)}) // No field, delete the hash
	}
	iter.Release()
}

// GetHash gets hash data
func (ldb *LevelDB) GetHash(key []byte) map[string][]byte {
	hash := make(map[string][]byte)

	hashPrefix := encodeHashFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	for iter.Next() {
		// Find the seperator '|'
		sepIndex := strings.IndexByte(string(iter.Key()), '|')
		// The field name should be the string after '|'
		key := append([]byte{}, iter.Key()[sepIndex+1:]...)
		value := append([]byte{}, iter.Value()...)
		hash[string(key)] = value
	}
	iter.Release()
	return hash
}

// GetHashFieldNames gets hash field names
func (ldb *LevelDB) GetHashFieldNames(key []byte) [][]byte {
	fields := [][]byte{}

	hashPrefix := encodeHashFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	for iter.Next() {
		// Find the seperator '|'
		sepIndex := strings.IndexByte(string(iter.Key()), '|')
		// The field name should be the string after '|'
		key := append([]byte{}, iter.Key()[sepIndex+1:]...)
		fields = append(fields, key)
	}
	iter.Release()
	return fields
}

// GetHashFieldNames gets hash fields
func (ldb *LevelDB) GetHashFields(key []byte, fields [][]byte) map[string][]byte {
	hash := make(map[string][]byte)
	for _, field := range fields {
		fieldValue := ldb.get(encodeHashFieldKey(key, field))
		hash[string(field)] = fieldValue
	}
	return hash
}

// GetHashFieldNames write hash data
func (ldb *LevelDB) PutHash(key []byte, hash map[string][]byte, expireAt *time.Time) {
	metaKey := encodeMetaKey(key)

	batch := new(leveldb.Batch)
	batch.Put(metaKey, encodeMetadata(Hash, expireAt))
	for k, v := range hash {
		fieldKey := encodeHashFieldKey(key, []byte(k))
		batch.Put(fieldKey, v)
	}
	if err := ldb.db.Write(batch, nil); err != nil {
		panic(err)
	}
}
