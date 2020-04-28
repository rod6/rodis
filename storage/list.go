// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb/util"
)

// encodeFieldKey encodes hash field key: -Key|Number
// Number=0 means the element to store attributes(length, head, tail, counter)
func encodeListElementKey(key []byte, num uint32) []byte {
	elementKey := make([]byte, 1 /* '-' */ +len(key)+1 /* '|' */ +4 /* num */)
	elementKey[0] = ValuePrefix
	copy(elementKey[1:], key)
	elementKey[1+len(key)] = Seperator
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, num)
	copy(elementKey[1+len(key)+1:], b)
	return elementKey
}

// DeleteList
func (ldb *LevelDB) DeleteList(key []byte) {
	keys := [][]byte{encodeMetaKey(key)}

	keyPrefix := make([]byte, 1+len(key))
	keyPrefix[0] = ValuePrefix
	copy(keyPrefix[1:], key)

	iter := ldb.db.NewIterator(util.BytesPrefix(keyPrefix), nil)
	for iter.Next() {
		key := append([]byte{}, iter.Key()...)
		keys = append(keys, key)
	}
	iter.Release()

	ldb.delete(keys)
}

// GetListAttr
func (ldb *LevelDB) GetListAttr(key []byte) (uint32, uint32, uint32, uint32) {
	m := ldb.get(encodeListElementKey(key, 0))
	if len(m) < 16 { //no attr or invalid
		return 0, 0, 0, 0
	}

	length := binary.BigEndian.Uint32(m[0:])
	head := binary.BigEndian.Uint32(m[4:])
	tail := binary.BigEndian.Uint32(m[8:])
	counter := binary.BigEndian.Uint32(m[12:])
	return length, head, tail, counter
}

// PutListAttr
func (ldb *LevelDB) PutListAttr(key []byte, length uint32, head uint32, tail uint32, counter uint32) {
	r := make([]byte, 4+4+4+4)
	binary.BigEndian.PutUint32(r[0:], length)
	binary.BigEndian.PutUint32(r[4:], head)
	binary.BigEndian.PutUint32(r[8:], tail)
	binary.BigEndian.PutUint32(r[12:], counter)

	ldb.put(encodeListElementKey(key, 0), r)
}

// PutListElement
func (ldb *LevelDB) PutListElement(key []byte, i uint32, next uint32, prev uint32, v []byte) {
	r := make([]byte, 4+4+len(v))
	binary.BigEndian.PutUint32(r[0:], next)
	binary.BigEndian.PutUint32(r[4:], prev)
	copy(r[8:], v)

	ldb.put(encodeListElementKey(key, i), r)
}

// GetListElement
func (ldb *LevelDB) GetListElement(key []byte, i uint32) (uint32, uint32, []byte) {
	r := ldb.get(encodeListElementKey(key, i))
	if r == nil || len(r) == 0 {
		return 0, 0, nil
	}

	next := binary.BigEndian.Uint32(r[0:])
	prev := binary.BigEndian.Uint32(r[4:])
	return next, prev, r[8:]
}

// DelListElement
func (ldb *LevelDB) DelListElement(key []byte, i uint32) {
	next, prev, v := ldb.GetListElement(key, i)
	ldb.delete([][]byte{encodeListElementKey(key, i)})

	if next == i {
		return
	}

	_, prevPrev, v := ldb.GetListElement(key, prev)
	ldb.PutListElement(key, prev, next, prevPrev, v)

	nextNext, _, v := ldb.GetListElement(key, next)
	ldb.PutListElement(key, next, nextNext, prev, v)
}

// SetListElement with index
func (ldb *LevelDB) SetListElement(key []byte, index int, v []byte) error {
	length, head, _, _ := ldb.GetListAttr(key)
	if index < 0 {
		index = index + int(length)
	}
	if index >= int(length) || index < 0 {
		return fmt.Errorf("ERR index out of range")
	}
	next := head
	for i := 0; i < index; i++ {
		next, _, _ = ldb.GetListElement(key, next)
	}
	curr := next
	next, prev, _ := ldb.GetListElement(key, curr)
	ldb.PutListElement(key, curr, next, prev, v)

	return nil
}

// GetListRange
func (ldb *LevelDB) GetListRange(key []byte, start int, end int) [][]byte {
	length, head, _, _ := ldb.GetListAttr(key)

	l := int(length)
	if start < 0 {
		start = l + start
	}
	if end < 0 {
		end = l + end
	}

	r := [][]byte{}

	if start < 0 {
		start = 0
	}
	if start >= l {
		return r
	}
	if end < 0 {
		end = 0
	}
	if end >= l {
		end = l - 1
	}
	if start > end {
		return r
	}

	next := head
	for i := 0; i < start; i++ {
		next, _, _ = ldb.GetListElement(key, next)
	}

	var v []byte
	for i := start; i <= end; i++ {
		curr := next
		next, _, v = ldb.GetListElement(key, curr)
		r = append(r, v)
	}
	return r
}

// TrimList
func (ldb *LevelDB) TrimList(key []byte, start int, end int) {
	length, head, tail, counter := ldb.GetListAttr(key)

	l := int(length)
	if start < 0 {
		start = l + start
	}
	if end < 0 {
		end = l + end
	}

	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= l {
		end = l - 1
	}

	if start >= l || start > end {
		ldb.DeleteList(key)
		return
	}

	trims := [][]byte{}
	next := head
	for i := 0; i < start; i++ {
		trims = append(trims, encodeListElementKey(key, next))
		next, _, _ = ldb.GetListElement(key, next)
	}

	newHead := next
	var v []byte
	for i := start; i < end; i++ {
		next, _, _ = ldb.GetListElement(key, next)
	}
	newTail := next

	for ; next != tail; next, _, _ = ldb.GetListElement(key, next) {
		trims = append(trims, encodeListElementKey(key, next))
	}

	ldb.delete(trims)
	next, prev, v := ldb.GetListElement(key, newHead)
	ldb.PutListElement(key, newHead, next, newTail, v)
	next, prev, v = ldb.GetListElement(key, newTail)
	ldb.PutListElement(key, newTail, newHead, prev, v)
	ldb.PutListAttr(key, uint32(end-start+1), newHead, newTail, counter)
}

// RemList
func (ldb *LevelDB) RemList(key []byte, count int, value []byte) int {
	if count == 0 {
		return 0
	}

	r := 0
	length, head, tail, counter := ldb.GetListAttr(key)

	curr := head
	if count < 0 {
		curr = tail
	}

	for access := 0; (access < int(length)) && (r < abs(count)); access++ {
		next, prev, v := ldb.GetListElement(key, curr)

		// if equal to the value
		if bytes.Equal(value, v) {
			r++
			// empty list, remove meta & attr, return r
			if r == int(length) {
				ldb.DeleteList(key)
				return r
			}

			// remove the element
			ldb.DelListElement(key, curr)

			if curr == head {
				head = next
			}
			if curr == tail {
				tail = prev
			}
		}

		if count > 0 {
			curr = next
		} else {
			curr = prev
		}
	}

	ldb.PutListAttr(key, length-uint32(r), head, tail, counter)
	return r
}

// GetLindexFromHead
func (ldb *LevelDB) GetLindexFromHead(key []byte, l uint32) []byte {
	length, head, _, _ := ldb.GetListAttr(key)
	if length < l+1 {
		return nil
	}

	next := head
	v := []byte{}

	for i := uint32(0); i < l; i++ {
		next, _, _ = ldb.GetListElement(key, next)
	}

	_, _, v = ldb.GetListElement(key, next)
	return v
}

// GetLindexFromTail
func (ldb *LevelDB) GetLindexFromTail(key []byte, l uint32) []byte {
	length, _, tail, _ := ldb.GetListAttr(key)
	if length < uint32(l+1) {
		return nil
	}

	prev := tail
	v := []byte{}

	for i := uint32(0); i < l; i++ {
		_, prev, _ = ldb.GetListElement(key, prev)
	}

	_, _, v = ldb.GetListElement(key, prev)
	return v
}

// InsertList
func (ldb *LevelDB) InsertList(key []byte, d string, pivot []byte, value []byte) int {
	length, head, tail, counter := ldb.GetListAttr(key)

	curr := head
	next := head
	prev := head
	v := []byte{}
	found := false
	var i uint32
	for i = 0; i < length; i++ {
		curr = next
		next, prev, v = ldb.GetListElement(key, next)
		if bytes.Equal(pivot, v) {
			found = true
			break
		}
	}

	if !found {
		return -1
	}

	counter++
	length++

	if d == "before" {
		ldb.PutListElement(key, counter, curr, prev, value)
		ldb.PutListElement(key, curr, next, counter, v)

		oldPrev := prev
		_, prev, v = ldb.GetListElement(key, prev)
		ldb.PutListElement(key, oldPrev, counter, prev, v)

		if curr == head {
			head = counter
		}

		ldb.PutListAttr(key, length, head, tail, counter)
	}

	if d == "after" {
		ldb.PutListElement(key, counter, next, curr, value)
		ldb.PutListElement(key, curr, counter, prev, v)

		oldNext := next
		next, _, v = ldb.GetListElement(key, next)
		ldb.PutListElement(key, oldNext, next, counter, v)

		if curr == tail {
			tail = counter
		}

		ldb.PutListAttr(key, length, head, tail, counter)
	}

	return int(length)
}

// PushListHead
func (ldb *LevelDB) PushListHead(key []byte, tipe byte, v []byte) uint32 {
	length, head, tail, counter := ldb.GetListAttr(key)

	length++
	counter++
	if length == 1 { // empty list
		ldb.put(encodeMetaKey(key), encodeMetadata(tipe))
		head = counter
		tail = counter
	}

	// insert new element to head
	ldb.PutListElement(key, counter, head, tail, v)

	// update previous head
	if length != 1 {
		headNext, _, headV := ldb.GetListElement(key, head)
		ldb.PutListElement(key, head, headNext, counter, headV)
	}

	// update tail
	if length != 1 {
		_, tailPrev, tailV := ldb.GetListElement(key, tail)
		ldb.PutListElement(key, tail, counter, tailPrev, tailV)
	}

	// update attr
	ldb.PutListAttr(key, length, counter, tail, counter)

	return length
}

// PushListTail
func (ldb *LevelDB) PushListTail(key []byte, tipe byte, v []byte) uint32 {
	length, head, tail, counter := ldb.GetListAttr(key)

	length++
	counter++
	if length == 1 { // empty list
		ldb.put(encodeMetaKey(key), encodeMetadata(tipe))
		head = counter
		tail = counter
	}

	// insert new element to tail
	ldb.PutListElement(key, counter, head, tail, v)

	// update head
	if length != 1 {
		headNext, _, headV := ldb.GetListElement(key, head)
		ldb.PutListElement(key, head, headNext, counter, headV)
	}

	// update previous tail
	if length != 1 {
		_, tailPrev, tailV := ldb.GetListElement(key, tail)
		ldb.PutListElement(key, tail, counter, tailPrev, tailV)
	}

	// update attr
	ldb.PutListAttr(key, length, head, counter, counter)

	return length
}

// PopListHead
func (ldb *LevelDB) PopListHead(key []byte) []byte {
	length, head, tail, counter := ldb.GetListAttr(key)

	if length == 0 {
		return nil
	}

	headNext, _, headV := ldb.GetListElement(key, head)
	if length == 1 {
		ldb.delete([][]byte{encodeMetaKey(key), encodeListElementKey(key, head), encodeListElementKey(key, 0)})
	} else {
		_, tailPrev, tailV := ldb.GetListElement(key, tail)
		ldb.PutListElement(key, tail, headNext, tailPrev, tailV)

		if headNext != tail {
			nextNext, _, nextV := ldb.GetListElement(key, headNext)
			ldb.PutListElement(key, headNext, nextNext, tail, nextV)
		}

		length--
		ldb.PutListAttr(key, length, headNext, tail, counter)
	}

	return headV
}

// PopListTail
func (ldb *LevelDB) PopListTail(key []byte) []byte {
	length, head, tail, counter := ldb.GetListAttr(key)

	if length == 0 {
		return nil
	}

	_, tailPrev, tailV := ldb.GetListElement(key, tail)
	if length == 1 {
		ldb.delete([][]byte{encodeMetaKey(key), encodeListElementKey(key, tail), encodeListElementKey(key, 0)})
	} else {
		headNext, _, headV := ldb.GetListElement(key, head)
		ldb.PutListElement(key, head, headNext, tailPrev, headV)

		if head != tailPrev {
			_, prevPrev, prevV := ldb.GetListElement(key, tailPrev)
			ldb.PutListElement(key, tailPrev, head, prevPrev, prevV)
		}

		length--
		ldb.PutListAttr(key, length, head, tailPrev, counter)
	}

	return tailV
}

// PopListHead
func (ldb *LevelDB) GetListLength(key []byte) uint32 {
	length, _, _, _ := ldb.GetListAttr(key)
	return length
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
