// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"errors"

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

func encodeMetadata(tipe byte) []byte {
	return []byte{MetaVersion, tipe}
}

func parseMetadata(metadata []byte) (byte, error) {
	if len(metadata) < 2 {
		return None, ErrMetaFormat
	}
	if metadata[0] != MetaVersion {
		return None, ErrMetaFormat
	}

	tipe := metadata[1]

	return tipe, nil
}
