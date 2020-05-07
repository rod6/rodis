// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package server

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"runtime"

	"github.com/libgo/logx"
	"github.com/pborman/uuid"

	"github.com/rod6/rodis/command"
	"github.com/rod6/rodis/resp"
	"github.com/rod6/rodis/storage"
)

type rodisConn struct {
	uuid   string
	db     *storage.LevelDB
	conn   net.Conn
	reader *bufio.Reader
	server *rodisServer
	buffer bytes.Buffer
	authed bool
	extras *command.Extras
}

func newConnection(conn net.Conn, rs *rodisServer) {
	uuid := uuid.New()
	rc := &rodisConn{
		uuid:   uuid,
		db:     storage.Select(0),
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: rs,
	}

	if rs.cfg.RequirePass == "" {
		rc.authed = true
	}

	rc.extras = &command.Extras{
		DB:       rc.db,
		Buffer:   &rc.buffer,
		Authed:   rc.authed,
		Password: rs.cfg.RequirePass,
	}

	rc.server.mu.Lock()
	rs.conns[uuid] = rc
	rc.server.mu.Unlock()

	logx.Debugf("New connection: %v", uuid)

	go rc.handle()
}

func (rc *rodisConn) handle() {
	for {
		respType, respValue, err := resp.Parse(rc.reader)
		if err != nil {
			select {
			case <-rc.server.quit: // Server is quit, rc.close() is called.
				return
			default:
				break
			}

			if err == io.EOF { // Client close the connection
				logx.Debugf("Client close connection %v.", rc.uuid)
				rc.close()
				return
			} else {
				logx.Errorf("Connection %v error: %v", rc.uuid, err)
				continue // Other error, should continue the connection
			}
		}

		rc.response(respType, respValue)
	}
}

func (rc *rodisConn) response(respType resp.RESPType, respValue resp.Value) {
	defer func() {
		if err := recover(); err != nil {
			stack := make([]byte, 2048)
			stack = stack[:runtime.Stack(stack, false)]
			logx.Errorf("Panic in handling connection %v, command is %v, err is %s\n%s", rc.uuid, respValue, err, stack)
			rc.conn.Write([]byte("-ERR server unknown error\r\n"))
		}
	}()

	if respType != resp.ArrayType { // All command from client should be RESPArrayType
		logx.Errorf("Connection %v get a WRONG format command from client.", rc.uuid)
		rc.conn.Write([]byte("-ERR wrong input format\r\n"))
		return
	}

	err := command.Handle(respValue.(resp.Array), rc.extras)
	if err != nil {
		logx.Errorf("Connection %v get a server error: %v", rc.uuid, err)
		rc.conn.Write([]byte("-ERR server unknown error\r\n"))
		return
	}

	rc.conn.Write(rc.buffer.Bytes())
}

func (rc *rodisConn) close() {
	err := rc.conn.Close()
	if err != nil {
		logx.Debugf("Connection %v close error: %v", rc.uuid, err)
	}

	rc.server.mu.Lock()
	delete(rc.server.conns, rc.uuid)
	rc.server.mu.Unlock()

	logx.Debugf("Connection %v closed.", rc.uuid)
}
