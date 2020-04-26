// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Because leveldb/rocksdb is a PURE key/value data engine, to store complex redis data type
// is not that simple.
// In Rodis, the solution is useing two+ kev/value entries for one redis key/value: one for meta
// data, others for value data. String data only one data entries, set/hash may have more than
// one value data entries.
// To explain the format, I use rKey (redis key) to represent the key from redis client, metaKey
// for the metadata key in leveldb/rocksdb, valueKey for the value data key.
//
// Metakey always has a prefix: '+', metaKey := '+' + rKey
// Metadata format:
//      first byte: meta data version
//      second byte: lower 4 bits: RedisType, upper 4 bits: if has expire value
//      remained bytes: attributes for different bytes
//
// Valuekey always has a prefix: '-'. For hash/set data type, use '|' as seperator.
//
// String Type:
//      +StringKey   -> metadata (only 2 bytes)
//      -StringKey   -> string value
//
// Hash Type:
//      +HashKey        -> metadata (only 2 bytes)
//      -HashKey|Field1 -> value1
//      -HashKey|Field2 -> value2
//      -HashKey|Field3 -> value3
//
// List Type:
//      +ListKey            -> metadata (2bytes + uint32 len for length of list + uint32 for head + uint32 for tail)
//      -ListKey|0x00000000 -> 0x00000001|0x00000000|0 (next|prev|value)
//      -ListKey|0x00000001 -> 0x00000001|0x00000000|1 (next|prev|value)
//
// Expire Hash: to store expire of keys, using time.Unix value
//      +SYSExpire -> metadata (as hash)
//      -SYSExpire|rKey -> time.Unix()
//

package storage
