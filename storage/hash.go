// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Field struct {
	Key   []byte
	Value []byte
}

// encodeFieldKey encodes hash field key
func encodeFieldKey(key []byte, field []byte) []byte {
	fieldKey := make([]byte, 1 /* '-' */ +len(key)+1 /* '|' */ +len(field))
	fieldKey[0] = ValuePrefix
	copy(fieldKey[1:], key)
	fieldKey[1+len(key)] = Seperator
	copy(fieldKey[1+len(key)+1:], field)
	return fieldKey
}

// DeleteHash deletes all hash data
func (ldb *LevelDB) DeleteHash(key []byte) {
	keys := [][]byte{encodeMetaKey(key)}

	// enum fields, and delete all
	hashPrefix := encodeFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	for iter.Next() {
		key := append([]byte{}, iter.Key()...)
		keys = append(keys, key)
	}
	iter.Release()
	ldb.delete(keys)
}

// DeleteHashFields deletes hash fields
func (ldb *LevelDB) DeleteFields(key []byte, fields [][]byte) {
	// Delete fields
	keys := make([][]byte, len(fields))
	for i, field := range fields {
		keys[i] = encodeFieldKey(key, field)
	}
	ldb.delete(keys)

	// After delete, remove the hash meta entry if no fields in this hash
	hashPrefix := encodeFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	if !iter.Next() {
		ldb.delete([][]byte{encodeMetaKey(key)}) // No field, delete the hash
	}
	iter.Release()
}

// GetHash gets hash data
func (ldb *LevelDB) GetHash(key []byte) map[string][]byte {
	hash := make(map[string][]byte)

	hashPrefix := encodeFieldKey(key, nil)
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

// GetHashAsArray gets hash data as array to ensure the insertion sort
func (ldb *LevelDB) GetHashAsArray(key []byte) []Field {
	hash := []Field{}

	hashPrefix := encodeFieldKey(key, nil)
	iter := ldb.db.NewIterator(util.BytesPrefix(hashPrefix), nil)
	for iter.Next() {
		// Find the seperator '|'
		sepIndex := strings.IndexByte(string(iter.Key()), '|')
		// The field name should be the string after '|'
		key := append([]byte{}, iter.Key()[sepIndex+1:]...)
		value := append([]byte{}, iter.Value()...)
		hash = append(hash, Field{key, value})
	}
	iter.Release()
	return hash
}

// GetHashFieldNames gets hash field names
func (ldb *LevelDB) GetFieldNames(key []byte) [][]byte {
	fields := [][]byte{}

	hashPrefix := encodeFieldKey(key, nil)
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
func (ldb *LevelDB) GetFields(key []byte, fields [][]byte) map[string][]byte {
	hash := make(map[string][]byte)
	for _, field := range fields {
		fieldValue := ldb.get(encodeFieldKey(key, field))
		hash[string(field)] = fieldValue
	}
	return hash
}

// GetHashFieldNamesAsArray gets hash fields as array
func (ldb *LevelDB) GetFieldsAsArray(key []byte, fields [][]byte) []Field {
	hash := []Field{}
	for _, field := range fields {
		value := ldb.get(encodeFieldKey(key, field))
		hash = append(hash, Field{field, value})
	}
	return hash
}

// GetHashFieldNames write hash data
func (ldb *LevelDB) PutHash(key []byte, hash map[string][]byte, expire bool) {
	metaKey := encodeMetaKey(key)

	batch := new(leveldb.Batch)
	batch.Put(metaKey, encodeMetadata(Hash, expire))
	for k, v := range hash {
		fieldKey := encodeFieldKey(key, []byte(k))
		batch.Put(fieldKey, v)
	}
	if err := ldb.db.Write(batch, nil); err != nil {
		panic(err)
	}
}
