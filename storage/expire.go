// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"encoding/binary"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

var (
	ExpireKey []byte = []byte("SYSExpire")
)

// encodeExpireKey encodes expire key as -SYSExpire|key
func encodeExpireKey(key []byte) []byte {
	expireKey := make([]byte, 1 /* '-' */ +len(ExpireKey)+1 /* '|' */ +len(key))
	expireKey[0] = ValuePrefix
	copy(expireKey[1:], ExpireKey)
	expireKey[1+len(ExpireKey)] = Seperator
	copy(expireKey[1+len(ExpireKey)+1:], key)
	return expireKey
}

// GetExpireAt returns expire as time.Time
func (ldb *LevelDB) GetExpireAt(key []byte) *time.Time {
	at := ldb.get(encodeExpireKey(key))
	var r time.Time
	if len(at) != 0 {
		r = time.Unix(int64(binary.BigEndian.Uint64(at)), 0)
	}
	return &r
}

// ClearExpireAt clears expire
func (ldb *LevelDB) ClearExpireAt(key []byte) {
	metaKey := encodeMetaKey(key)
	expireKey := encodeExpireKey(key)
	has, tipe, expire := ldb.Has(key)
	if !has || !expire {
		return
	}

	batch := new(leveldb.Batch)
	batch.Put(metaKey, encodeMetadata(tipe, false))
	batch.Delete(expireKey)
	if err := ldb.db.Write(batch, nil); err != nil {
		panic(err)
	}
	return
}

// SetExpireAt stores the value to expire
func (ldb *LevelDB) SetExpireAt(key []byte, at *time.Time) {
	if at == nil || at.IsZero() {
		return
	}

	expireKey := encodeExpireKey(key)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(at.Unix()))
	ldb.put(expireKey, buf)
}

// SetExpireInMeta set expire flag in meta data.
func (ldb *LevelDB) SetExpireInMeta(key []byte, tipe byte) {
	ldb.put(encodeMetaKey(key), encodeMetadata(tipe, true))
}
