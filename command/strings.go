// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

// Implement for command list in http://redis.io/commands#string
//
// command		status
// ---------------------
// APPEND		done
// BITCOUNT		done
// BITFIELD     TODO
// BITOP		done
// BITPOS		done
// DECR			done
// DECRBY		done
// GET			done
// GETBIT		done
// GETRANGE		done
// GETSET		done
// INCR			done
// INCRBY		done
// INCRBYFLOAT	done
// MGET			done
// MSET			done
// MSETNX		done
// PSETEX		done
// SET			done
// SETBIT		done
// SETEX		done
// SETNX		done
// SETRANGE		done
// STRLEN		done

const STRLIMIT = 536870912 // 512M

// appendx -> https://redis.io/commands/append
func appendx(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	val := []byte("")
	if exist {
		val = ex.DB.GetString(v[0])
	}
	if len(val)+len(v[1]) > STRLIMIT {
		return resp.NewError(ErrStringExccedLimit).WriteTo(ex.Buffer)
	}

	val = append(val, v[1]...)
	ex.DB.PutString(v[0], val)
	return resp.Integer(len(val)).WriteTo(ex.Buffer)
}

// bitcount -> https://redis.io/commands/bitcount
func bitcount(v resp.CommandArgs, ex *Extras) error {
	if len(v) == 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "bitcount").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	if len(v) != 1 && len(v) != 3 {
		return resp.NewError(ErrFmtSyntax).WriteTo(ex.Buffer)
	}

	val := ex.DB.GetString(v[0])

	start := 0
	end := len(val)
	var err error

	if len(v) == 3 {
		start, err = strconv.Atoi(string(v[1]))
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}

		end, err = strconv.Atoi(string(v[2]))
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}

		start, end = calcRange(start, end, len(val))
	}

	if end <= start {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	sum := 0
	for _, b := range val[start:end] {
		sum += countSetBits[b]
	}
	return resp.Integer(sum).WriteTo(ex.Buffer)
}

// bitop -> https://redis.io/commands/bitop
func bitop(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 3 {
		return resp.NewError(ErrFmtWrongNumberArgument, "bitop").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	op := strings.ToLower(string(v[0]))

	switch op {
	case "not":
		if len(v) > 3 {
			return resp.NewError(ErrBitOPNotError).WriteTo(ex.Buffer)
		}
		exist, tipe := ex.DB.Has(v[2])
		if !exist {
			return resp.ZeroInteger.WriteTo(ex.Buffer)
		}
		if exist && tipe != storage.String {
			return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
		}

		val := ex.DB.GetString(v[2])
		destValue := make([]byte, len(val))
		for i, b := range val {
			destValue[i] = ^b
		}

		ex.DB.PutString(v[1], destValue)
		return resp.Integer(len(destValue)).WriteTo(ex.Buffer)

	case "or", "and", "xor":
		var destValue []byte = nil
		for _, b := range v[2:] {
			exist, tipe := ex.DB.Has(b)
			if exist && tipe != storage.String {
				return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
			}
			val := ex.DB.GetString(b)
			if exist && len(destValue) < len(val) {
				if len(destValue) == 0 { // loop first step
					destValue = append(destValue, val...)
					continue
				} else {
					destValue = append(destValue, make([]byte, len(val)-len(destValue))...)
				}
			}
			for i := range destValue {
				s := byte(0)
				if exist && i < len(val) {
					s = val[i]
				}
				switch op {
				case "or":
					destValue[i] |= s
				case "and":
					destValue[i] &= s
				case "xor":
					destValue[i] ^= s
				}
			}
		}
		ex.DB.PutString(v[1], destValue)
		return resp.Integer(len(destValue)).WriteTo(ex.Buffer)

	default:
		return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
	}
}

// bitpos -> https://redis.io/commands/bitpos
func bitpos(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 2 {
		return resp.NewError(ErrFmtWrongNumberArgument, "bitpos").WriteTo(ex.Buffer)
	}

	arg, err := strconv.Atoi(string(v[1]))
	if err != nil || arg != 0 && arg != 1 {
		return resp.NewError(ErrShouldBe0or1).WriteTo(ex.Buffer)
	}

	set := arg == 1   // set bit pos
	clear := arg == 0 // clear bit pos

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	// This is the same behavior as offical redis. Not sure why
	// not check the len(v) when key is missing
	if !exist && set {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}
	if !exist && clear {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	// Seam that: check the len(v) only when the key exist
	if len(v) > 4 {
		return resp.NewError(ErrFmtSyntax).WriteTo(ex.Buffer)
	}

	val := ex.DB.GetString(v[0])
	// Get the range.
	start := 0
	end := len(val)
	if len(v) >= 3 {
		start, err = strconv.Atoi(string(v[2]))
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}
	}
	if len(v) == 4 {
		end, err = strconv.Atoi(string(v[3]))
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}
	}
	start, end = calcRange(start, end, len(val))
	if end <= start {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}

	// Get the postion in the range
	pos := 0
	found := false
	for _, b := range val[start:end] {
		if clear && posFirstClear[b] != 8 {
			found = true
			pos += posFirstClear[b]
			break
		}
		if set && posFirstSet[b] != -1 {
			found = true
			pos += posFirstSet[b]
			break
		}
		pos += 8 // not found, pos += 1*byte
	}

	if found {
		return resp.Integer(8*start + pos).WriteTo(ex.Buffer)
	}

	// From http://redis.io/commands/bitpos
	// If we look for set bits (the bit argument is 1) and the string is
	// empty or composed of just zero bytes, -1 is returned.
	if !found && set {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}

	// If we look for clear bits (the bit argument is 0) and the string only
	// contains bit set to 1, the function returns the first bit not part of
	// the string on the right. So if the string is three bytes set to the
	// value 0xff the command BITPOS key 0 will return 24, since up to bit 23
	// all the bits are 1.
	// Basically, the function considers the right of the string as padded with
	// zeros if you look for clear bits and specify no range or the start argument
	// only.
	if !found && clear && len(v) < 4 { //len(v) < 4: no range 'end' specified
		return resp.Integer(8 * end).WriteTo(ex.Buffer)
	}
	// However, this behavior changes if you are looking for clear bits and
	// specify a range with both start and end. If no clear bit is found in
	// the specified range, the function returns -1 as the user specified a
	// clear range and there are no 0 bits in that range.
	if !found && clear && len(v) == 4 {
		return resp.NegativeOneInteger.WriteTo(ex.Buffer)
	}
	return resp.NegativeOneInteger.WriteTo(ex.Buffer) // Should NEVER called
}

// decr -> https://redis.io/commands/decr
func decr(v resp.CommandArgs, ex *Extras) error {
	return incrdecrHelper(v, ex, -1)
}

// decrby -> https://redis.io/commands/decrby
func decrby(v resp.CommandArgs, ex *Extras) error {
	by, err := strconv.ParseInt(v[1].String(), 10, 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	return incrdecrHelper(v, ex, by*-1)
}

// get -> https://redis.io/commands/get
func get(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}
	val := ex.DB.GetString(v[0])
	return resp.BulkString(val).WriteTo(ex.Buffer)
}

// getbit -> https://redis.io/commands/getbit
func getbit(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	val := ex.DB.GetString(v[0])

	offset, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	if offset >= 8*len(val) {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	byten := offset / 8
	pos := offset % 8

	k := val[byten] >> uint32(7-pos) & 0x01
	return resp.Integer(k).WriteTo(ex.Buffer)
}

// getrange -> https://redis.io/commands/getrange
func getrange(v resp.CommandArgs, ex *Extras) error {
	start, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	end, err := strconv.Atoi(string(v[2]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyBulkString.WriteTo(ex.Buffer)
	}
	if tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	val := ex.DB.GetString(v[0])
	start, end = calcRange(start, end, len(val))
	if end <= start {
		return resp.EmptyBulkString.WriteTo(ex.Buffer)
	}

	return resp.BulkString(val[start:end]).WriteTo(ex.Buffer)
}

// getset -> https://redis.io/commands/getset
func getset(v resp.CommandArgs, ex *Extras) error {
	if len(v[1]) > STRLIMIT {
		return resp.NewError(ErrStringExccedLimit).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}
	var oldValue []byte
	if exist {
		oldValue = ex.DB.GetString(v[0])
	}

	ex.DB.PutString(v[0], v[1])

	if !exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	return resp.BulkString(oldValue).WriteTo(ex.Buffer)
}

// incr -> https://redis.io/commands/incr
func incr(v resp.CommandArgs, ex *Extras) error {
	return incrdecrHelper(v, ex, 1)
}

// incrby -> https://redis.io/commands/incrby
func incrby(v resp.CommandArgs, ex *Extras) error {
	by, err := strconv.ParseInt(v[1].String(), 10, 64)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	return incrdecrHelper(v, ex, by)
}

// incrbyfloat -> https://redis.io/commands/incrbyfloat
func incrbyfloat(v resp.CommandArgs, ex *Extras) error {
	by, err := strconv.ParseFloat(v[1].String(), 64)
	if err != nil {
		return resp.NewError(ErrNotValidFloat).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	newVal := 0.0
	if !exist {
		newVal += by
	} else {
		val := ex.DB.GetString(v[0])
		f, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return resp.NewError(ErrNotValidFloat).WriteTo(ex.Buffer)
		}
		newVal = f + by
	}

	s := []byte(strconv.FormatFloat(newVal, 'f', -1, 64))
	ex.DB.PutString(v[0], s)
	return resp.BulkString(s).WriteTo(ex.Buffer)
}

// mget -> https://redis.io/commands/mget
func mget(v resp.CommandArgs, ex *Extras) error {
	if len(v) < 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "mget").WriteTo(ex.Buffer)
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	arr := make(resp.Array, len(v))
	for i, g := range v {
		exist, tipe := ex.DB.Has(g)
		if !exist || tipe != storage.String {
			arr[i] = resp.NilBulkString
		} else {
			val := ex.DB.GetString(g)
			arr[i] = resp.BulkString(val)
		}
	}

	return arr.WriteTo(ex.Buffer)
}

// mset -> https://redis.io/commands/mset
func mset(v resp.CommandArgs, ex *Extras) error {
	if len(v) == 0 || len(v)%2 != 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "mset").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for i := 0; i < len(v); {
		ex.DB.PutString(v[i], v[i+1])
		i += 2
	}

	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// msetnx -> https://redis.io/commands/msetnx
func msetnx(v resp.CommandArgs, ex *Extras) error {
	if len(v) == 0 || len(v)%2 != 0 {
		return resp.NewError(ErrFmtWrongNumberArgument, "msetnx").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	for i := 0; i < len(v); {
		exist, _ := ex.DB.Has(v[i])
		if exist {
			return resp.ZeroInteger.WriteTo(ex.Buffer) // If any key exist, return 0
		}
		i += 2
	}

	for i := 0; i < len(v); { // every key does not exist, put all into level db.
		ex.DB.PutString(v[i], v[i+1])
		i += 2
	}

	return resp.OneInteger.WriteTo(ex.Buffer)
}

// psetex -> https://redis.io/commands/psetex
func psetex(v resp.CommandArgs, ex *Extras) error {
	expire, err := strconv.ParseInt(string(v[1]), 10, 32)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	at := time.Now().Add(time.Duration(expire) * time.Millisecond)

	ex.DB.PutString(v[0], v[2])
	ex.DB.SetExpireAt(v[0], &at)

	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// set -> https://redis.io/commands/set
func set(v resp.CommandArgs, ex *Extras) error {
	if len(v) <= 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "set").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	if len(v) == 2 {
		ex.DB.PutString(v[0], v[1])
		ex.DB.ClearExpireAt(v[0])
		return resp.OkSimpleString.WriteTo(ex.Buffer)
	}

	optionNx := false
	optionXx := false
	optionEx := false
	expireOp := ""
	expireVal := int64(0)

	offset := 2
	for offset < len(v) {
		option := strings.ToLower(string(v[offset]))
		switch option {
		case "xx":
			optionXx = true
			offset++
		case "nx":
			optionNx = true
			offset++
		case "ex", "px":
			optionEx = true
			if offset == len(v)-1 { // no value more
				return resp.NewError(ErrFmtSyntax).WriteTo(ex.Buffer)
			}
			if i, err := strconv.ParseInt(string(v[offset+1]), 10, 64); err != nil {
				return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
			} else {
				expireOp = option
				expireVal = i
			}
			offset += 2
		default:
			return resp.NewError(ErrFmtSyntax).WriteTo(ex.Buffer)
		}
	}

	if optionNx && optionXx {
		return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
	}

	exist, _ := ex.DB.Has(v[0])
	if optionNx && exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if optionXx && !exist {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	if len(v[1]) > STRLIMIT {
		return resp.NewError(ErrStringExccedLimit).WriteTo(ex.Buffer)
	}

	if !optionEx {
		ex.DB.PutString(v[0], v[1])
		return resp.OkSimpleString.WriteTo(ex.Buffer)
	}

	var at time.Time
	switch expireOp {
	case "ex":
		at = time.Now().Add(time.Duration(expireVal) * time.Second)
	case "px":
		at = time.Now().Add(time.Duration(expireVal) * time.Millisecond)
	}

	ex.DB.PutString(v[0], v[1])
	ex.DB.SetExpireAt(v[0], &at)

	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// setbit -> https://redis.io/commands/setbit
func setbit(v resp.CommandArgs, ex *Extras) error {
	i64, err := strconv.ParseInt(string(v[1]), 10, 32)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	offset := uint32(i64)
	pos := offset % 8
	byten := offset / 8

	if int(byten)+1 > STRLIMIT {
		return resp.NewError(ErrStringExccedLimit).WriteTo(ex.Buffer)
	}

	bit, err := strconv.Atoi(string(v[2]))
	if err != nil || bit != 0 && bit != 1 {
		return resp.NewError(ErrBitValueInvalid).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	val := []byte("")
	if exist {
		val = ex.DB.GetString(v[0])
	}

	if uint32(len(val)) < byten+1 {
		val = append(val, make([]byte, int(byten)+1-len(val))...)
	}

	k := val[byten] >> uint32(7-pos) & 0x01

	switch bit {
	case 0:
		clear := byte(^(0x01 << (7 - pos)))
		val[byten] = val[byten] & clear
	case 1:
		set := byte(0x01 << (7 - pos))
		val[byten] = val[byten] | set
	}

	ex.DB.PutString(v[0], val)
	return resp.Integer(k).WriteTo(ex.Buffer)
}

// setex -> https://redis.io/commands/setex
func setex(v resp.CommandArgs, ex *Extras) error {
	expire, err := strconv.ParseInt(string(v[1]), 10, 32)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	at := time.Now().Add(time.Duration(expire) * time.Second)

	ex.DB.PutString(v[0], v[2])
	ex.DB.SetExpireAt(v[0], &at)

	return resp.OkSimpleString.WriteTo(ex.Buffer)
}

// setnx -> https://redis.io/commands/setnx
func setnx(v resp.CommandArgs, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, _ := ex.DB.Has(v[0])
	if exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}

	ex.DB.PutString(v[0], v[1])
	return resp.OneInteger.WriteTo(ex.Buffer)

}

// setrange -> https://redis.io/commands/setrange
func setrange(v resp.CommandArgs, ex *Extras) error {
	i64, err := strconv.ParseInt(string(v[1]), 10, 32)
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	offset := int(i64)
	if offset < 0 {
		return resp.NewError(ErrOffsetOutRange).WriteTo(ex.Buffer)
	}
	if offset+len(v[2]) > 536870912 { // 512M is the limit length
		return resp.NewError(ErrStringExccedLimit).WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	val := []byte("")
	if exist {
		val = ex.DB.GetString(v[0])
	}

	if len(val) < offset+len(v[2]) {
		val = append(val, make([]byte, len(v[2])+offset-len(val))...)
	}
	copy(val[offset:], v[2])

	ex.DB.PutString(v[0], val)
	return resp.Integer(len(val)).WriteTo(ex.Buffer)
}

// strlen -> https://redis.io/commands/strlen
func strlen(v resp.CommandArgs, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.ZeroInteger.WriteTo(ex.Buffer)
	}
	if tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	val := ex.DB.GetString(v[0])
	return resp.Integer(len(val)).WriteTo(ex.Buffer)
}

// strings.helper
var countSetBits = [256]int{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
}

var posFirstSet = [256]int{ // -1: no 1 in this byte
	-1, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

var posFirstClear = [256]int{ // -1: no 0 in this byte
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 7, 8,
}

func calcRange(start, end, len int) (int, int) {
	switch {
	case start >= len:
		start = len
	case start <= -1*len:
		start = 0
	case start < 0 && start > -1*len:
		start = start + len
	}

	switch {
	case end >= len:
		end = len
	case end <= -1*len:
		end = 1
	case end < 0 && start > -1*len:
		end = len + end + 1
	default:
		end += 1
	}

	return start, end
}

func incrdecrHelper(v resp.CommandArgs, ex *Extras, by int64) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != storage.String {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	newVal := int64(0)
	if !exist {
		newVal += by
	} else {
		val := ex.DB.GetString(v[0])
		i, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
		}
		newVal = i + by
	}

	ex.DB.PutString(v[0], []byte(strconv.FormatInt(newVal, 10)))
	return resp.Integer(newVal).WriteTo(ex.Buffer)
}
