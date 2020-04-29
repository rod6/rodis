// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"math/rand"

	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

// Implement for command list in http://redis.io/commands#set
//
// command		status
// ---------------------
// SADD         done
// SCARD        done
// SDIFF        done
// SDIFFSTORE   done
// SINTER       done
// SINTERSTORE  done
// SISMEMBER    done
// SMEMBERS     done
// SMOVE        done
// SPOP         partly done, no count support
// SRANDMEMBER  partly done, no count support
// SREM         done
// SSCAN        TODO
// SUNION       done
// SUNIONSTORE  done

// sadd -> https://redis.io/commands/sadd
func sadd(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sadd").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := make(map[string][]byte)
	for _, s := range v[1:] {
		hash[string(s)] = []byte("set")
	}
	ex.DB.PutHash(v[0], storage.Set, hash)
	// TODO: sadd returns the number that added to the set
	// Now, we return the number of args, will update later.
	return resp.Integer(len(v[1:])).WriteTo(ex.Buffer)
}

// scard -> https://redis.io/commands/scard
func scard(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != storage.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	elements := ex.DB.GetFieldNames(v[0])
	return resp.Integer(len(elements)).WriteTo(ex.Buffer)
}

// sdiff -> https://redis.io/commands/sdiff
func sdiff(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sdiff").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for i, s := range v {
		exist, tipe := ex.DB.Has(s)
		if i == 0 && !exist { // first key not exists, return empty
			return resp.EmptyArray.WriteTo(ex.Buffer)
		}
		if exist && tipe != storage.Set {
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
func sdiffstore(v resp.CommandArgs, ex *Extras) error {
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
		if exist && tipe != storage.Set {
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
		ex.DB.PutHash(v[0], storage.Set, set0)
	}
	return resp.Integer(len(set0)).WriteTo(ex.Buffer)
}

// sinter -> https://redis.io/commands/sinter
func sinter(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sinter").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for _, s := range v {
		exist, tipe := ex.DB.Has(s)
		if !exist { // first key not exists, return empty
			return resp.EmptyArray.WriteTo(ex.Buffer)
		}
		if tipe != storage.Set {
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
func sinterstore(v resp.CommandArgs, ex *Extras) error {
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
		if exist && tipe != storage.Set {
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
		ex.DB.PutHash(v[0], storage.Set, set0)
	}
	return resp.Integer(len(set0)).WriteTo(ex.Buffer)
}

// sismember -> https://redis.io/commands/sismember
func sismember(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if exist && tipe != storage.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[1]})
	if _, ok := hash[string(v[1])]; !ok {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	return resp.OneInteger.WriteTo(ex.Buffer)
}

// smembers -> https://redis.io/commands/smembers
func smembers(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if tipe != storage.Set {
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
func smove(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != storage.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}
	exist, tipe = ex.DB.Has(v[1])
	if exist && tipe != storage.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	hash := ex.DB.GetFields(v[0], [][]byte{v[2]})
	if _, ok := hash[string(v[2])]; !ok {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	ex.DB.DeleteFields(v[0], [][]byte{v[2]})

	ex.DB.PutHash(v[1], storage.Set, hash)
	return resp.OneInteger.WriteTo(ex.Buffer)
}

// spop -> https://redis.io/commands/spop
func spop(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if tipe != storage.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	elements := ex.DB.GetFieldNames(v[0])

	i := rand.Intn(len(elements))
	ex.DB.DeleteFields(v[0], [][]byte{elements[i]})
	return resp.BulkString(elements[i]).WriteTo(ex.Buffer)
}

// srandmember -> https://redis.io/commands/srandmember
func srandmember(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if tipe != storage.Set {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	elements := ex.DB.GetFieldNames(v[0])

	i := rand.Intn(len(elements))
	return resp.BulkString(elements[i]).WriteTo(ex.Buffer)
}

// srem -> https://redis.io/commands/srem
func srem(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "srem").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != storage.Set {
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
func sunion(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sunion").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for _, s := range v {
		exist, tipe := ex.DB.Has(s)
		if exist && tipe != storage.Set {
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
func sunionstore(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "sunionstore").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for _, s := range v[1:] {
		exist, tipe := ex.DB.Has(s)
		if exist && tipe != storage.Set {
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
		ex.DB.PutHash(v[0], storage.Set, set0)
	}
	return resp.Integer(len(set0)).WriteTo(ex.Buffer)
}
