// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"strconv"

	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

// Implement for command list in http://redis.io/commands#hash
//
// command		status		author		todo
// --------------------------------------------------
// HDEL         done        Rod
// HEXISTS      done        Rod
// HGET         done        rod
// HGETALL      done        rod
// HINCRBY      done        rod
// HINCRBYFLOAT done        rod
// HKEYS        done        rod
// HLEN         done        rod
// HMGET        done        rod
// HMSET        done        rod
// HSCAN        done        rod
// HSET         done        rod
// HSETNX       done        rod
// HSTRLEN      done        rod
// HVALS        done        rod

// hdel -> https://redis.io/commands/hdel
func hdel(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "hdel").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fields := [][]byte{}
	for _, field := range v[1:] {
		fields = append(fields, []byte(field))
	}
	hash := ex.DB.GetFields(v[0], fields)

	count := 0
	for _, value := range hash {
		if len(value) != 0 {
			count++
		}
	}
	ex.DB.DeleteFields(v[0], fields)
	return resp.Integer(count).WriteTo(ex.Buffer)
}

// hexists -> https://redis.io/commands/hexist
func hexists(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})
	if len(hash[string(v[1])]) == 0 {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	return resp.OneInteger.WriteTo(ex.Buffer)
}

// hget -> https://redis.io/commands/hget
func hget(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})
	if len(hash[string(v[1])]) == 0 {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}

	return resp.BulkString(hash[string(v[1])]).WriteTo(ex.Buffer)
}

// hgetall -> https://redis.io/commands/hgetall
func hgetall(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetHashAsArray(v[0])
	arr := resp.Array{}

	for _, field := range hash {
		arr = append(arr, resp.BulkString(field.Key), resp.BulkString(field.Value))
	}
	return arr.WriteTo(ex.Buffer)
}

// hincrby -> https://redis.io/commands/hincrby
func hincrby(v resp.CommandArgs, ex *Extras) error {
	by, err := strconv.ParseInt(v[2].String(), 10, 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	keyExists, tipe, expire := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})

	newVal := int64(0)
	if len(hash[string(v[1])]) == 0 {
		newVal += by
	} else {
		i, err := strconv.ParseInt(string(hash[string(v[1])]), 10, 64)
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}
		newVal = i + by
	}
	hash[string(v[1])] = []byte(strconv.FormatInt(newVal, 10))

	ex.DB.PutHash(v[0], hash, expire)
	return resp.Integer(newVal).WriteTo(ex.Buffer)
}

// hincrbyfloat -> https://redis.io/commands/hincrbyfloat
func hincrbyfloat(v resp.CommandArgs, ex *Extras) error {
	by, err := strconv.ParseFloat(v[2].String(), 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe, expire := ex.DB.Has(v[0])
	if exist && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})

	newVal := 0.0
	if len(hash[string(v[1])]) == 0 {
		newVal += by
	} else {
		f, err := strconv.ParseFloat(string(hash[string(v[1])]), 64)
		if err != nil {
			return resp.NewError(ErrNotValidFloat).WriteTo(ex.Buffer)
		}
		newVal = f + by
	}
	hash[string(v[1])] = []byte(strconv.FormatFloat(newVal, 'f', -1, 64))

	ex.DB.PutHash(v[0], hash, expire)
	return resp.BulkString(hash[string(v[1])]).WriteTo(ex.Buffer)
}

// hkeys -> https://redis.io/commands/hkeys
func hkeys(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fields := ex.DB.GetFieldNames(v[0])
	arr := resp.Array{}

	for _, field := range fields {
		arr = append(arr, resp.BulkString(field))
	}
	return arr.WriteTo(ex.Buffer)
}

// hvals -> https://redis.io/commands/hvals
func hvals(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetHashAsArray(v[0])
	arr := resp.Array{}

	for _, field := range hash {
		arr = append(arr, resp.BulkString(field.Value))
	}
	return arr.WriteTo(ex.Buffer)
}

// hlen -> https://redis.io/commands/hlen
func hlen(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fields := ex.DB.GetFieldNames(v[0])
	return resp.Integer(len(fields)).WriteTo(ex.Buffer)
}

// hmget -> https://redis.io/commands/hmget
func hmget(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "hmget").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fields := v[1:].ToBytes()
	hash := ex.DB.GetFieldsAsArray(v[0], fields)

	arr := resp.Array{}
	for _, field := range hash {
		if len(field.Value) == 0 {
			arr = append(arr, resp.NilBulkString)
		} else {
			arr = append(arr, resp.BulkString(field.Value))
		}
	}
	return arr.WriteTo(ex.Buffer)
}

// hmset -> https://redis.io/commands/hmset
func hmset(v resp.CommandArgs, ex *Extras) error {
	if len(v) <= 1 || len(v)%2 != 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "hmset").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe, expire := ex.DB.Has(v[0])
	if exist && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := make(map[string][]byte)
	for i := 1; i < len(v); {
		hash[string(v[i])] = v[i+1]
		i += 2
	}
	ex.DB.PutHash(v[0], hash, expire)
	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// hset -> https://redis.io/commands/hset
func hset(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe, expire := ex.DB.Has(v[0])
	if exist && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fieldExists := false
	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})
	if len(hash[string(v[1])]) != 0 {
		fieldExists = true
	}

	hash[string(v[1])] = v[2]
	ex.DB.PutHash(v[0], hash, expire)

	if !fieldExists {
		return resp.OneInteger.WriteTo(ex.Buffer)
	}
	return resp.ZeroInteger.WriteTo(ex.Buffer)
}

// hsetnx -> https://redis.io/commands/hsetnx
func hsetnx(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe, expire := ex.DB.Has(v[0])
	if exist && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	fieldExists := false
	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})
	if len(hash[string(v[1])]) != 0 {
		fieldExists = true
	}

	if !fieldExists {
		hash[string(v[1])] = v[2]
		ex.DB.PutHash(v[0], hash, expire)
		return resp.OneInteger.WriteTo(ex.Buffer)
	}
	return resp.ZeroInteger.WriteTo(ex.Buffer)
}

// hstrlen -> https://redis.io/commands/hstrlen
func hstrlen(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	keyExists, tipe, _ := ex.DB.Has(v[0])
	if !keyExists {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if keyExists && tipe != storage.Hash {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})
	return resp.Integer(len(hash[string(v[1])])).WriteTo(ex.Buffer)
}
