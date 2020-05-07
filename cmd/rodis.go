// Copyright (c) 2020, Rod Dong <rod.dong@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by The MIT License.

package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/libgo/logx"
	"github.com/rod6/rodis/server"
	"github.com/rod6/rodis/storage"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	configFile := flag.String("c", "rodis.toml", "Rodis config file path")
	flag.Parse()

	logx.SetOutput(logx.FileWriter(logx.FileConfig{
		Format:   "json",
		Level:    logx.InfoLevel,
		Filename: "rodis.log"}), logx.StdWriter(logx.StdConfig{Level: logx.DebugLevel}))

	if err := server.LoadConfig(*configFile); err != nil {
		logx.Fatalf("Load/Parse config file error: %v", err)
	}

	err := storage.Open(server.Config.LevelDBPath, server.Config.LevelDB)
	if err != nil {
		logx.Fatalf("Open storage error: %v", err)
	}
	defer storage.Close()

	rs, err := server.New(server.Config)
	if err != nil {
		logx.Fatalf("New server error: %v", err)
	}
	defer rs.Close()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go rs.Run()
	<-sc
}
