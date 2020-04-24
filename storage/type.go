// Copyright (c) 2015, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"errors"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

// https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
// A one byte flag indicates encoding used to save the Value
const (
	String             byte = 0
	List               byte = 1
	Set                byte = 2
	SortedSet          byte = 3
	Hash               byte = 4
	Zipmap             byte = 9
	Ziplist            byte = 10
	Intset             byte = 11
	SortedSetInZiplist byte = 12
	HashmapInZiplist   byte = 13
	None               byte = 0xFF
)

var TypeString = map[byte]string{
	String:             "string",
	List:               "list",
	Set:                "set",
	SortedSet:          "zset",
	Hash:               "hash",
	Zipmap:             "zmap",
	Ziplist:            "list",
	Intset:             "set",
	SortedSetInZiplist: "list",
	HashmapInZiplist:   "list",
	None:               "none",
}

type RodisData interface {
	GetWriteBatch() *leveldb.Batch
}

const (
	MetaVersion byte = 0x00
	MetaPrefix  byte = '+'
	ValuePrefix byte = '-'
	Seperator   byte = '|'
)

var (
	ErrMetaFormat = errors.New("Meta data format is wrong")
)

func encodeMetaKey(key []byte) []byte {
	metaKey := make([]byte, len(key)+1)
	metaKey[0] = MetaPrefix
	copy(metaKey[1:], key)
	return metaKey
}

func encodeMetadata(tipe byte, expireAt *time.Time) []byte {
	if expireAt == nil || expireAt.IsZero() {
		return []byte{MetaVersion, tipe}
	}

	expire, _ := expireAt.MarshalBinary()
	metadata := make([]byte, 1 /*version*/ +1 /*hasExpire + type*/ +len(expire))
	metadata[0] = MetaVersion
	metadata[1] = 0x10 | tipe
	copy(metadata[2:], expire)

	return metadata
}

func parseMetadata(metadata []byte) (byte, *time.Time, error) {
	if len(metadata) < 2 {
		return None, nil, ErrMetaFormat
	}
	if metadata[0] != MetaVersion {
		return None, nil, ErrMetaFormat
	}

	tipe := metadata[1] & 0x0F // lower 4 bits of metadata[1] is type
	hasExpire := metadata[1]&byte(0xF0) == byte(0x10)

	if !hasExpire {
		return tipe, nil, nil
	}

	var expireAt time.Time
	err := expireAt.UnmarshalBinary(metadata[2:])
	if err != nil {
		return None, nil, ErrMetaFormat
	}

	return tipe, &expireAt, nil
}

func encodeStringKey(key []byte) []byte {
	valueKey := make([]byte, 1 /* '+' */ +len(key))
	valueKey[0] = ValuePrefix
	copy(valueKey[1:], key)
	return valueKey
}

func encodeHashFieldKey(key []byte, field []byte) []byte {
	fieldKey := make([]byte, 1 /* '-' */ +len(key)+1 /* '|' */ +len(field))
	fieldKey[0] = ValuePrefix
	copy(fieldKey[1:], key)
	fieldKey[1+len(key)] = Seperator
	copy(fieldKey[1+len(key)+1:], field)
	return fieldKey
}
