// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"math/rand"

	"github.com/rod6/rodis/resp"
)

// command
// -----------
// SADD
// SCARD
// SDIFF
// SDIFFSTORE
// SINTER
// SINTERSTORE
// SISMEMBER
// SMEMBERS
// SMOVE
// SPOP         (partly done, no count support)
// SRANDMEMBER  (partly done, no count support)
// SREM
// SUNION
// SUNIONSTORE

// sadd -> https://redis.io/commands/sadd
func sadd(v Args, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sadd").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := make(map[string][]byte)
	for _, s := range v[1:] {
		hash[string(s)] = []byte("set")
	}
	ex.DB.PutHash(v[0], resp.Set, hash)
	// TODO: sadd returns the number that added to the set
	// Now, we return the number of Args, will update later.
	return resp.Integer(len(v[1:])).WriteTo(ex.Buffer)
}

// scard -> https://redis.io/commands/scard
func scard(v Args, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	elements := ex.DB.GetFieldNames(v[0])
	return resp.Integer(len(elements)).WriteTo(ex.Buffer)
}

// sdiff -> https://redis.io/commands/sdiff
func sdiff(v Args, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sdiff").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	for i, s := range v {
		exist, tipe := ex.DB.Has(s)
		if i == 0 && !exist { // first key not exists, return empty
			return resp.EmptyArray.WriteTo(ex.Buffer)
		}
		if exist && tipe != resp.Set {
			return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
		}
	}

	set0 := ex.DB.GetHash(v[0])
	for _, s := range v[1:] {
		setx := ex.DB.GetHash(s)
		for element := range setx {
			delete(set0, element)
		}
	}
	arr := resp.Array{}
	for element := range set0 {
		arr = append(arr, resp.BulkString(element))
	}
	return arr.WriteTo(ex.Buffer)
}

// sdiffstore -> https://redis.io/commands/sdiffstore
func sdiffstore(v Args, ex *Extras) error {
	if len(v) < 3 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sdiffstore").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for i, s := range v[1:] {
		exist, tipe := ex.DB.Has(s)
		if i == 0 && !exist { // first key not exists, return empty
			return resp.ZeroInteger.WriteTo(ex.Buffer)
		}
		if exist && tipe != resp.Set {
			return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
		}
	}

	exist, _ := ex.DB.Has(v[0])
	if exist {
		del(v[0:0], ex)
	}

	set0 := ex.DB.GetHash(v[1])
	for _, s := range v[2:] {
		setx := ex.DB.GetHash(s)
		for element := range setx {
			delete(set0, element)
		}
	}
	if len(set0) > 0 {
		ex.DB.PutHash(v[0], resp.Set, set0)
	}
	return resp.Integer(len(set0)).WriteTo(ex.Buffer)
}

// sinter -> https://redis.io/commands/sinter
func sinter(v Args, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sinter").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	for _, s := range v {
		exist, tipe := ex.DB.Has(s)
		if !exist { // first key not exists, return empty
			return resp.EmptyArray.WriteTo(ex.Buffer)
		}
		if tipe != resp.Set {
			return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
		}
	}

	// inter
	set0 := ex.DB.GetHash(v[0])
	for _, s := range v[1:] {
		setx := ex.DB.GetHash(s)
		for element := range set0 {
			if _, ok := setx[element]; !ok {
				delete(set0, element)
			}
		}
	}

	arr := resp.Array{}
	for element := range set0 {
		arr = append(arr, resp.BulkString(element))
	}
	return arr.WriteTo(ex.Buffer)
}

// sinterstore -> https://redis.io/commands/sinterstore
func sinterstore(v Args, ex *Extras) error {
	if len(v) < 3 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sinterstore").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for i, s := range v[1:] {
		exist, tipe := ex.DB.Has(s)
		if i == 0 && !exist { // first key not exists, return empty
			return resp.ZeroInteger.WriteTo(ex.Buffer)
		}
		if exist && tipe != resp.Set {
			return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
		}
	}

	exist, _ := ex.DB.Has(v[0])
	if exist {
		del(v[0:0], ex)
	}

	// inter
	set0 := ex.DB.GetHash(v[0])
	for _, s := range v[1:] {
		setx := ex.DB.GetHash(s)
		for element := range set0 {
			if _, ok := setx[element]; !ok {
				delete(set0, element)
			}
		}
	}

	if len(set0) > 0 {
		ex.DB.PutHash(v[0], resp.Set, set0)
	}
	return resp.Integer(len(set0)).WriteTo(ex.Buffer)
}

// sismember -> https://redis.io/commands/sismember
func sismember(v Args, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if exist && tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})
	if _, ok := hash[string(v[1])]; !ok {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	return resp.OneInteger.WriteTo(ex.Buffer)
}

// smembers -> https://redis.io/commands/smembers
func smembers(v Args, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	elements := ex.DB.GetFieldNames(v[0])
	arr := resp.Array{}

	for _, element := range elements {
		arr = append(arr, resp.BulkString(element))
	}
	return arr.WriteTo(ex.Buffer)
}

// smove -> https://redis.io/commands/smove
func smove(v Args, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}
	exist, tipe = ex.DB.Has(v[1])
	if exist && tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[2]})
	if _, ok := hash[string(v[2])]; !ok {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	ex.DB.DeleteFields(v[0], [][]byte{v[2]})

	ex.DB.PutHash(v[1], resp.Set, hash)
	return resp.OneInteger.WriteTo(ex.Buffer)
}

// spop -> https://redis.io/commands/spop
func spop(v Args, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	elements := ex.DB.GetFieldNames(v[0])

	i := rand.Intn(len(elements))
	ex.DB.DeleteFields(v[0], [][]byte{elements[i]})
	return resp.BulkString(elements[i]).WriteTo(ex.Buffer)
}

// srandmember -> https://redis.io/commands/srandmember
func srandmember(v Args, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	elements := ex.DB.GetFieldNames(v[0])

	i := rand.Intn(len(elements))
	return resp.BulkString(elements[i]).WriteTo(ex.Buffer)
}

// srem -> https://redis.io/commands/srem
func srem(v Args, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "srem").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != resp.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	elements := [][]byte{}
	for _, element := range v[1:] {
		elements = append(elements, []byte(element))
	}
	hash := ex.DB.GetFields(v[0], elements)

	count := 0
	for _, value := range hash {
		if len(value) != 0 {
			count++
		}
	}
	ex.DB.DeleteFields(v[0], elements)
	return resp.Integer(count).WriteTo(ex.Buffer)
}

// sunion -> https://redis.io/commands/sunion
func sunion(v Args, ex *Extras) error {
	if len(v) < 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sunion").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	for _, s := range v {
		exist, tipe := ex.DB.Has(s)
		if exist && tipe != resp.Set {
			return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
		}
	}

	// union
	set0 := ex.DB.GetHash(v[0])
	for _, s := range v[1:] {
		setx := ex.DB.GetHash(s)
		for element := range setx {
			set0[element] = setx[element]
		}
	}

	arr := resp.Array{}
	for element := range set0 {
		arr = append(arr, resp.BulkString(element))
	}
	return arr.WriteTo(ex.Buffer)
}

// sunionstore -> https://redis.io/commands/sunionstore
func sunionstore(v Args, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sunionstore").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for _, s := range v[1:] {
		exist, tipe := ex.DB.Has(s)
		if exist && tipe != resp.Set {
			return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
		}
	}

	exist, _ := ex.DB.Has(v[0])
	if exist {
		del(v[0:0], ex)
	}

	// union
	set0 := ex.DB.GetHash(v[0])
	for _, s := range v[1:] {
		setx := ex.DB.GetHash(s)
		for element := range setx {
			set0[element] = setx[element]
		}
	}

	if len(set0) > 0 {
		ex.DB.PutHash(v[0], resp.Set, set0)
	}
	return resp.Integer(len(set0)).WriteTo(ex.Buffer)
}
