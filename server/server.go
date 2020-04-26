// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package server

import (
	"net"
	"sync"

	"github.com/libgo/logx"
	"github.com/rod6/rodis/config"
)

type rodisServer struct {
	cfg      *config.RodisConfig
	listener net.Listener
	conns    map[string]*rodisConn
	mu       sync.Mutex
	started  bool
	quit     chan bool
}

func New(config config.RodisConfig) (*rodisServer, error) {
	return &rodisServer{cfg: &config, conns: make(map[string]*rodisConn), quit: make(chan bool)}, nil
}

func (rs *rodisServer) Run() {
	logx.Infof("Server is starting, listen on %v", rs.cfg.Listen)

	listener, err := net.Listen("tcp", rs.cfg.Listen)
	if err != nil {
		logx.Fatalf("Server listen on %v failure: %v", rs.cfg.Listen, err)
		return
	}

	rs.listener = listener
	rs.started = true

	for {
		conn, err := rs.listener.Accept()
		if err != nil {
			select {
			case <-rs.quit:
				return
			default:
				logx.Warnf("Server accepts connection error: %v", err)
			}
			continue
		}

		go newConnection(conn, rs)
	}
}

func (rs *rodisServer) Close() {
	logx.Info("Server is closing...")
	if rs.started {
		close(rs.quit)
		rs.listener.Close()

		for _, rc := range rs.conns {
			rc.close()
		}
		rs.started = false
	}
	logx.Info("Server is down.")
}
