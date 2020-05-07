// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

// Package command is to handle the command from client.
package command

import (
	"strconv"
	"strings"

	"github.com/rod6/rodis/resp"
)

// command
// ------------
// ZADD
// ZCARD
// ZRANGE
// ZRANGEBYSCORE
// ZRANK
// ZREM

// zadd -> https://redis.io/commands/zadd
func zadd(v Args, ex *Extras) error {
	if len(v) <= 1 || len(v)%2 != 1 {
		return resp.NewError(ErrFmtWrongNumberArgument, "zadd").WriteTo(ex.Buffer)
	}

	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != resp.SortedSet {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	for i := 1; i < len(v); {
		score, err := strconv.ParseFloat(string(v[i]), 64)
		if err != nil {
			return resp.NewError(ErrNotValidFloat).WriteTo(ex.Buffer)
		}
		field := v[i+1]

		ex.DB.AddSkipField(v[0], resp.SortedSet, field, score)
		i += 2
	}
	return resp.Integer(len(v) / 2).WriteTo(ex.Buffer)
}

// zcard -> https://redis.io/commands/zcard
func zcard(v Args, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != resp.SortedSet {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	return resp.Integer(ex.DB.GetSkipLength(v[0])).WriteTo(ex.Buffer)
}

// zrange -> https://redis.io/commands/zrange
func zrange(v Args, ex *Extras) error {
	if len(v) != 3 && len(v) != 4 {
		return resp.NewError(ErrFmtWrongNumberArgument, "range").WriteTo(ex.Buffer)
	}

	withscores := false
	if len(v) == 4 {
		if strings.ToLower(string(v[3])) != "withscores" {
			return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
		}
		withscores = true
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if exist && tipe != resp.SortedSet {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	start, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}
	end, err := strconv.Atoi(string(v[2]))
	if err != nil {
		return resp.NewError(ErrNotValidInt).WriteTo(ex.Buffer)
	}

	arr := resp.Array{}
	elements := ex.DB.GetSkipRange(v[0], start, end)

	for _, element := range elements {
		arr = append(arr, resp.BulkString(element.Field))
		if withscores {
			arr = append(arr, resp.BulkString(strconv.FormatFloat(element.Score, 'f', -1, 64)))
		}
	}
	return arr.WriteTo(ex.Buffer)
}

// zrangebyscore -> https://redis.io/commands/zrangebyscore
func zrangebyscore(v Args, ex *Extras) error {
	if len(v) != 3 && len(v) != 4 {
		return resp.NewError(ErrFmtWrongNumberArgument, "zrangebyscore").WriteTo(ex.Buffer)
	}

	minex := false
	maxex := false
	min := float64(0.0)
	max := float64(0.0)

	minb := v[1]
	if v[1][0] == '(' {
		minex = true
		minb = v[1][1:]
		if len(minb) == 0 {
			return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
		}
	}
	score, err := strconv.ParseFloat(string(minb), 64)
	if err != nil {
		return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
	}
	min = score

	maxb := v[2]
	if v[2][0] == '(' {
		maxex = true
		maxb = v[2][1:]
		if len(maxb) == 0 {
			return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
		}
	}
	score, err = strconv.ParseFloat(string(maxb), 64)
	if err != nil {
		return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
	}
	max = score

	withscores := false
	if len(v) == 4 {
		if strings.ToLower(string(v[3])) != "withscores" {
			return resp.NewError(ErrSyntax).WriteTo(ex.Buffer)
		}
		withscores = true
	}

	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if !exist {
		return resp.EmptyArray.WriteTo(ex.Buffer)
	}
	if exist && tipe != resp.SortedSet {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	arr := resp.Array{}
	elements := ex.DB.GetSkipRangeByScore(v[0], min, minex, max, maxex)

	for _, element := range elements {
		arr = append(arr, resp.BulkString(element.Field))
		if withscores {
			arr = append(arr, resp.BulkString(strconv.FormatFloat(element.Score, 'f', -1, 64)))
		}
	}
	return arr.WriteTo(ex.Buffer)
}

// zrank -> https://redis.io/commands/zrank
func zrank(v Args, ex *Extras) error {
	ex.DB.RLock()
	defer ex.DB.RUnlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != resp.SortedSet {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}

	r, err := ex.DB.GetSkipFieldRank(v[0], v[1])
	if err != nil {
		return resp.NilBulkString.WriteTo(ex.Buffer)
	}
	return resp.Integer(r).WriteTo(ex.Buffer)
}

// zrem -> https://redis.io/commands/zrem
func zrem(v Args, ex *Extras) error {
	ex.DB.Lock()
	defer ex.DB.Unlock()

	exist, tipe := ex.DB.Has(v[0])
	if exist && tipe != resp.SortedSet {
		return resp.NewError(ErrWrongType).WriteTo(ex.Buffer)
	}

	r := ex.DB.DeleteSkipField(v[0], v[1])
	return resp.Integer(r).WriteTo(ex.Buffer)
}
