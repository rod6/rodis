// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"errors"

	"github.com/rod6/rodis/resp"
	"github.com/syndtr/goleveldb/leveldb"
)

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
		return resp.None, ErrMetaFormat
	}
	if metadata[0] != MetaVersion {
		return resp.None, ErrMetaFormat
	}

	tipe := metadata[1]
	return tipe, nil
}
