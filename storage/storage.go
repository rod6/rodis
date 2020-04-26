// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
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

type LevelDB struct {
	db  *leveldb.DB
	rwm *sync.RWMutex
}

const STRBYTE byte = 0x00

var ErrLevelDB = errors.New("Backend Level DB Error")
var ErrNotFound = leveldb.ErrNotFound

// open/exist/get/put/delete/close are helpers for access internal data of leveldb.
func open(dbPath string, options *opt.Options) (*LevelDB, error) {
	db, err := leveldb.OpenFile(dbPath, options)
	if err != nil {
		return nil, err
	}

	var rwmutex sync.RWMutex

	return &LevelDB{db: db, rwm: &rwmutex}, nil
}

func (ldb *LevelDB) has(metaKey []byte) (bool, byte, bool) {
	metadata, err := ldb.db.Get(metaKey, nil)

	if err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}

	if err == leveldb.ErrNotFound {
		return false, None, false
	}

	tipe, expire, err := parseMetadata(metadata)
	if err != nil {
		panic(err)
	}
	return true, tipe, expire
}

func (ldb *LevelDB) delete(keys [][]byte) {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		batch.Delete(key)
	}
	if err := ldb.db.Write(batch, nil); err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}
}

func (ldb *LevelDB) get(key []byte) []byte {
	value, err := ldb.db.Get(key, nil)
	if err != nil && err != ErrNotFound {
		panic(err)
	}
	return value
}

func (ldb *LevelDB) put(key []byte, value []byte) {
	err := ldb.db.Put(key, value, nil)
	if err != nil {
		panic(err)
	}
}

func (ldb *LevelDB) close() {
	if ldb.db != nil {
		ldb.db.Close()
	}
}

// Flush is to flush leveldb
func (ldb *LevelDB) Flush() error {
	iter := ldb.db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		ldb.db.Delete(key, nil)
	}
	iter.Release()
	return iter.Error()
}

// Has is to determine if a key exists
func (ldb *LevelDB) Has(key []byte) (bool, byte, bool) {
	metaKey := encodeMetaKey(key)
	exist, tipe, expire := ldb.has(metaKey)

	if !exist || !expire {
		return exist, tipe, expire
	}

	at := ldb.GetExpireAt(key)
	if at.After(time.Now()) {
		return true, tipe, true
	}

	switch tipe {
	case String:
		ldb.DeleteString(key)
	case Hash:
		ldb.DeleteHash(key)
	}

	return false, tipe, false
}

// Lock/Unlock functions
func (ldb *LevelDB) RLock() {
	ldb.rwm.RLock()
}
func (ldb *LevelDB) RUnlock() {
	ldb.rwm.RUnlock()
}
func (ldb *LevelDB) Lock() {
	ldb.rwm.Lock()
}
func (ldb *LevelDB) Unlock() {
	ldb.rwm.Unlock()
}
