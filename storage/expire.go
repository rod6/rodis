// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"encoding/binary"
	"time"
)

var (
	ExpireKey []byte = []byte("SYSExpire")
)

// encodeExpireKey encodes expire key as -SYSExpire|key
func encodeExpireKey(key []byte) []byte {
	expireKey := []byte{ValuePrefix}
	expireKey = append(expireKey, ExpireKey...)
	expireKey = append(expireKey, Seperator)
	expireKey = append(expireKey, key...)
	return expireKey
}

// GetExpireAt returns expire as time.Time
func (ldb *LevelDB) GetExpireAt(key []byte) *time.Time {
	at := ldb.get(encodeExpireKey(key))
	// no expire, return nil
	if len(at) == 0 {
		return nil
	}

	r := time.Unix(int64(binary.BigEndian.Uint64(at)), 0)
	return &r
}

// ClearExpireAt clears expire
func (ldb *LevelDB) ClearExpireAt(key []byte) {
	ldb.delete([][]byte{encodeExpireKey(key)})
}

// SetExpireAt stores the value to expire
func (ldb *LevelDB) SetExpireAt(key []byte, at *time.Time) {
	if at == nil || at.IsZero() {
		return
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(at.Unix()))
	ldb.put(encodeExpireKey(key), buf)
}
