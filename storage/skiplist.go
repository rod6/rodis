// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>

// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/libgo/logx"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	SKIPLISTMAXLEVEL = 32
	SKIPLISTP        = 0.25
)

var (
	SKIPATTR = []byte{0x00, 0x00, 0x00, 0x00}
	SKIPHEAD = []byte{0x00, 0x00, 0x00, 0x01}
)

type SkipListElement struct {
	Field []byte
	Score float64
}

type skipListLevel struct {
	forward []byte
	span    uint32
}

type skipListAttr struct {
	length uint32
	level  uint32
	tail   []byte
}

type skipListNode struct {
	field    []byte
	score    float64
	backward []byte
	levels   []skipListLevel
}

// encodeSkipFieldKey encodes hash field key: -Key|Field
// Number=0 means the element to store attributes(length, head, tail, counter)
func encodeSkipFieldKey(key []byte, field []byte) []byte {
	fieldKey := make([]byte, 1 /* '-' */ +len(key)+1 /* '|' */ +len(field) /* num */)
	fieldKey[0] = ValuePrefix
	copy(fieldKey[1:], key)
	fieldKey[1+len(key)] = Seperator
	copy(fieldKey[1+len(key)+1:], field)
	return fieldKey
}

// DeleteSkip
func (ldb *LevelDB) DeleteSkip(key []byte) {
	keys := [][]byte{encodeMetaKey(key)}

	keyPrefix := append([]byte{ValuePrefix}, key...)
	iter := ldb.db.NewIterator(util.BytesPrefix(keyPrefix), nil)
	for iter.Next() {
		keys = append(keys, append([]byte{}, iter.Key()...))
	}
	iter.Release()

	ldb.delete(keys)
}

// deleteSkipNode
func (ldb *LevelDB) deleteSkipNode(key []byte, attr *skipListAttr, head *skipListNode, node *skipListNode, update []*skipListNode) {
	for i := 0; i < int(attr.level); i++ {
		if bytes.Equal(update[i].levels[i].forward, node.field) {
			update[i].levels[i].span = update[i].levels[i].span + node.levels[i].span - 1
			update[i].levels[i].forward = node.levels[i].forward
		} else {
			update[i].levels[i].span = update[i].levels[i].span - 1
		}
	}

	for _, element := range update {
		ldb.putSkipNode(key, element)
	}

	if node.levels[0].forward != nil {
		forward := ldb.getSkipNode(key, node.levels[0].forward)
		forward.backward = node.backward
		ldb.putSkipNode(key, forward)
	} else {
		attr.tail = node.backward
	}

	for attr.level > 1 {
		if head.levels[attr.level-1].forward == nil {
			attr.level = attr.level - 1
		} else {
			break
		}
	}
	attr.length = attr.length - 1

	if attr.length == 0 {
		ldb.DeleteSkip(key)
	} else {
		ldb.putSkipNode(key, head)
		ldb.putSkipAttr(key, attr)
		ldb.delete([][]byte{encodeSkipFieldKey(key, node.field)})
	}
}

// dumpSkip
func (ldb *LevelDB) dumpSkip(key []byte) {
	attr := ldb.getSkipAttr(key)
	if key == nil {
		logx.Infof("skiplist [%v] attr == nil", string(key))
		return
	}

	logx.Infof("skiplist [%v] attr == [length=%v, level=%v, tail=%v]", string(key), attr.length, attr.level, string(attr.tail))

	head := ldb.getSkipNode(key, SKIPHEAD)
	dumpNode(head)

	next := head.levels[0].forward
	for next != nil {
		node := ldb.getSkipNode(key, next)
		dumpNode(node)
		next = node.levels[0].forward
	}
}

func dumpNode(n *skipListNode) {
	if n == nil {
		return
	}

	logx.Infof("skipnode [%v], score=%v, backward=%v, level[0]=%v(%v), level[1]=%v(%v), level[2]=%v(%v)", string(n.field), n.score, string(n.backward), string(n.levels[0].forward), n.levels[0].span, string(n.levels[1].forward), n.levels[1].span, string(n.levels[2].forward), n.levels[2].span)
}

// GetSkipLength
func (ldb *LevelDB) GetSkipLength(key []byte) uint32 {
	attr := ldb.getSkipAttr(key)
	if attr == nil {
		return 0
	}
	return attr.length
}

// getSkipAttr
func (ldb *LevelDB) getSkipAttr(key []byte) *skipListAttr {
	m := ldb.get(encodeSkipFieldKey(key, []byte{0x00, 0x00, 0x00, 0x00}))
	if len(m) < 5 { //no attr or invalid
		return nil
	}

	length := binary.BigEndian.Uint32(m[0:])
	level := binary.BigEndian.Uint32(m[4:])
	tailL := uint8(m[8])
	var tail []byte = nil
	if tailL != 0 {
		tail = m[9 : 9+tailL]
	}
	return &skipListAttr{length, level, tail}
}

// putSkipAttr
func (ldb *LevelDB) putSkipAttr(key []byte, attr *skipListAttr) {
	attrKey := encodeSkipFieldKey(key, []byte{0x00, 0x00, 0x00, 0x00})

	m := make([]byte, 8)
	binary.BigEndian.PutUint32(m, attr.length)
	binary.BigEndian.PutUint32(m[4:], attr.level)

	m = append(m, uint8(len(attr.tail)))
	m = append(m, attr.tail...)

	ldb.put(attrKey, m)
}

// getSkipNode
func (ldb *LevelDB) getSkipNode(key []byte, field []byte) *skipListNode {
	m := ldb.get(encodeSkipFieldKey(key, field))
	if len(m) == 0 {
		return nil
	}

	score := byteToFloat64(m)
	var backward []byte = nil
	backwardL := uint8(m[8])
	if backwardL != 0 {
		backward = m[9 : 9+backwardL]
	}

	cursor := 9 + backwardL
	levels := []skipListLevel{}
	for i := 0; i < SKIPLISTMAXLEVEL; i++ {
		l := uint8(m[cursor])
		cursor++
		if l == 0 {
			levels = append(levels, skipListLevel{nil, 0})
			continue
		}
		forward := m[cursor : cursor+l]
		cursor += l

		span := binary.BigEndian.Uint32(m[cursor:])
		cursor += 4

		levels = append(levels, skipListLevel{forward, span})
	}
	r := skipListNode{field, score, backward, levels}
	return &r
}

// putSkipNode
func (ldb *LevelDB) putSkipNode(key []byte, node *skipListNode) {
	if node == nil {
		return
	}
	nodeKey := encodeSkipFieldKey(key, node.field)

	m := float64ToByte(node.score)
	m = append(m, uint8(len(node.backward)))
	m = append(m, node.backward...)

	for _, level := range node.levels {
		m = append(m, uint8(len(level.forward)))
		if len(level.forward) != 0 {
			m = append(m, level.forward...)
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, level.span)
			m = append(m, b...)
		}
	}

	ldb.put(nodeKey, m)
}

// initSkipNode
func (ldb *LevelDB) initSkipNode(field []byte) *skipListNode {
	levels := []skipListLevel{}
	for i := 0; i < SKIPLISTMAXLEVEL; i++ {
		levels = append(levels, skipListLevel{nil, 0})
	}

	return &skipListNode{field, float64(0.0), nil, levels}
}

// AddSkipField
func (ldb *LevelDB) AddSkipField(key []byte, tipe byte, field []byte, score float64) {
	attr := ldb.getSkipAttr(key)
	if attr == nil {
		ldb.put(encodeMetaKey(key), encodeMetadata(tipe))
		attr = &skipListAttr{0, 1, nil}
		ldb.putSkipAttr(key, attr)

		ldb.putSkipNode(key, ldb.initSkipNode(SKIPHEAD))
	}

	head := ldb.getSkipNode(key, SKIPHEAD)

	update := make([]*skipListNode, SKIPLISTMAXLEVEL)
	rank := make([]uint32, SKIPLISTMAXLEVEL)

	node := head
	for i := int(attr.level - 1); i >= 0; i-- {
		if int(attr.level-1) == i {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for node.levels[i].forward != nil {
			forward := ldb.getSkipNode(key, node.levels[i].forward)
			if forward.score < score || (forward.score == score && bytes.Compare(forward.field, field) < 0) {
				rank[i] = rank[i] + node.levels[i].span
				node = forward
			} else {
				break
			}
		}
		update[i] = node
	}

	level := randomLevel()
	if level > attr.level {
		for i := attr.level; i < level; i++ {
			rank[i] = 0
			update[i] = head
			update[i].levels[i].span = attr.length
		}
		attr.level = level
	}

	node = ldb.initSkipNode(field)
	node.score = score
	for i := uint32(0); i < level; i++ {
		node.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = node.field

		node.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < attr.level; i++ {
		update[i].levels[i].span = update[i].levels[i].span + 1
	}

	if update[0] != head {
		node.backward = update[0].field
	}

	if len(node.levels[0].forward) != 0 {
		forward := ldb.getSkipNode(key, node.levels[0].forward)
		forward.backward = node.field
		ldb.putSkipNode(key, forward)
	} else {
		attr.tail = node.field
	}

	attr.length = attr.length + 1
	ldb.putSkipAttr(key, attr)
	ldb.putSkipNode(key, node)
	for _, u := range update {
		if u != nil {
			ldb.putSkipNode(key, u)
		}
	}
}

// GetSkipFieldRank
func (ldb *LevelDB) GetSkipFieldRank(key []byte, field []byte) (int, error) {
	x := ldb.getSkipNode(key, field)
	if x == nil {
		return 0, fmt.Errorf("Not found this field")
	}

	attr := ldb.getSkipAttr(key)
	if attr == nil {
		return 0, fmt.Errorf("Not found this zset")
	}

	head := ldb.getSkipNode(key, SKIPHEAD)
	rank := make([]uint32, SKIPLISTMAXLEVEL)

	node := head
	for i := int(attr.level - 1); i >= 0; i-- {
		if int(attr.level-1) == i {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for node.levels[i].forward != nil {
			forward := ldb.getSkipNode(key, node.levels[i].forward)
			if forward.score < x.score || (forward.score == x.score && bytes.Compare(forward.field, field) < 0) {
				rank[i] = rank[i] + node.levels[i].span
				node = forward
			} else {
				break
			}
		}
	}

	return int(rank[0]), nil
}

// DeleteSkipField
func (ldb *LevelDB) DeleteSkipField(key []byte, field []byte) int {
	attr := ldb.getSkipAttr(key)
	if attr == nil {
		return 0
	}
	node := ldb.getSkipNode(key, field)
	if node == nil {
		return 0
	}

	head := ldb.getSkipNode(key, SKIPHEAD)
	update := make([]*skipListNode, SKIPLISTMAXLEVEL)

	x := head
	for i := int(attr.level - 1); i >= 0; i-- {
		for x.levels[i].forward != nil {
			forward := ldb.getSkipNode(key, x.levels[i].forward)
			if forward.score < node.score || (forward.score == node.score && bytes.Compare(forward.field, field) < 0) {
				x = forward
			} else {
				break
			}
		}
		update[i] = x
	}

	ldb.deleteSkipNode(key, attr, head, node, update)
	return 1
}

// GetSkipRange
func (ldb *LevelDB) GetSkipRange(key []byte, start int, end int) []SkipListElement {
	attr := ldb.getSkipAttr(key)

	l := int(attr.length)
	if start < 0 {
		start = l + start
	}
	if end < 0 {
		end = l + end
	}

	r := []SkipListElement{}

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

	node := ldb.getSkipNode(key, SKIPHEAD)
	forward := node.levels[0].forward
	for i := 0; i < start; i++ {
		if forward == nil {
			return r
		}
		node = ldb.getSkipNode(key, forward)
		forward = node.levels[0].forward
	}

	for i := start; i <= end; i++ {
		if forward == nil {
			return r
		}
		node = ldb.getSkipNode(key, forward)
		forward = node.levels[0].forward
		r = append(r, SkipListElement{node.field, node.score})
	}
	return r
}

// GetSkipRangeByScore
func (ldb *LevelDB) GetSkipRangeByScore(key []byte, min float64, minex bool, max float64, maxex bool) []SkipListElement {
	r := []SkipListElement{}

	if min > max {
		return r
	}

	if min == max && (minex || maxex) {
		return r
	}

	attr := ldb.getSkipAttr(key)
	if attr == nil {
		return r
	}

	node := ldb.getSkipNode(key, SKIPHEAD)
	for i := int(attr.level - 1); i >= 0; i-- {
		for node.levels[i].forward != nil {
			forward := ldb.getSkipNode(key, node.levels[i].forward)
			if !scoreGteMin(forward.score, min, minex) {
				node = forward
			} else {
				break
			}
		}
	}

	node = ldb.getSkipNode(key, node.levels[0].forward)
	for node != nil {
		if scoreLteMax(node.score, max, maxex) {
			r = append(r, SkipListElement{node.field, node.score})
			node = ldb.getSkipNode(key, node.levels[0].forward)
		} else {
			break
		}
	}
	return r
}

func randomLevel() uint32 {
	level := uint32(1)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		v := r.Uint32()
		if float32(v&0xFFFF) > float32(SKIPLISTP*0xFFFF) {
			break
		}
		level++
	}
	if level > SKIPLISTMAXLEVEL {
		return SKIPLISTMAXLEVEL
	}
	return level
}

func float64ToByte(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func byteToFloat64(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}

func scoreGteMin(score float64, min float64, minex bool) bool {
	if !minex {
		return score > min
	}

	return score >= min
}

func scoreLteMax(score float64, max float64, maxex bool) bool {
	if !maxex {
		return score < max
	}

	return score <= max
}
