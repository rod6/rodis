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
//
// Valuekey always has a prefix: '-'. For hash/set data type, use '|' as seperator.
//
// String Type:
//      +StringKey   -> metadata (2 bytes)
//      -StringKey   -> string value
//
// Hash Type:
//      +HashKey        -> metadata (2 bytes)
//      -HashKey|Field1 -> value1
//      -HashKey|Field2 -> value2
//      -HashKey|Field3 -> value3
//
// List Type:
//      +ListKey            -> metadata (2 bytes)
//      -ListKey|0x00000000 -> attrdata (4 bytes for length + 4 bytes for head + 4 bytes for tail + 4 bytes for counter)
//      -ListKey|0x00000001 -> 0x00000002|0x00000003|item1 (next|prev|value)
//      -ListKey|0x00000002 -> 0x00000003|0x00000001|item2 (next|prev|value)
//      -ListKey|0x00000003 -> 0x00000000|0x00000002|item3 (next|prev|value)
//
// Set Type:
//      Using hash as the internal data structure, with the value = []byte{"set"}
//
// Expire Hash: to store expire of keys, using time.Unix value
//      +SYSExpire -> metadata (as hash)
//      -SYSExpire|rKey -> time.Unix()
//

package storage
