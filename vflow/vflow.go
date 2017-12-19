//: ----------------------------------------------------------------------------
//: Copyright (C) 2017 Verizon.  All Rights Reserved.
//: All Rights Reserved
//:
//: file:    vflow.go
//: details: TODO
//: author:  Mehrdad Arshad Rad
//: date:    02/01/2017
//:
//: Licensed under the Apache License, Version 2.0 (the "License");
//: you may not use this file except in compliance with the License.
//: You may obtain a copy of the License at
//:
//:     http://www.apache.org/licenses/LICENSE-2.0
//:
//: Unless required by applicable law or agreed to in writing, software
//: distributed under the License is distributed on an "AS IS" BASIS,
//: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//: See the License for the specific language governing permissions and
//: limitations under the License.
//: ----------------------------------------------------------------------------

// Package main is the vflow binary
package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"github.com/pkg/profile"
)

var (
	opts   *Options
	logger *log.Logger
)

type proto interface {
	run()
	shutdown()
}

func main() {
	var (
		wg       sync.WaitGroup
		signalCh = make(chan os.Signal, 1)
	)
	
	p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)

	opts = GetOptions()
	runtime.GOMAXPROCS(opts.GetCPU())

	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	sFlow := NewSFlow()
	ipfix := NewIPFIX()
	netflow9 := NewNetflowV9()
	ipfix_tcp := NewIPFIXTCP()

	protos := []proto{sFlow, ipfix, netflow9, ipfix_tcp}

	for _, p := range protos {
		wg.Add(1)
		go func(p proto) {
			defer wg.Done()
			p.run()
		}(p)
	}

	//go statsHTTPServer(ipfix, sFlow, netflow9)

	<-signalCh

	for _, p := range protos {
		wg.Add(1)
		go func(p proto) {
			defer wg.Done()
			p.shutdown()
		}(p)
	}
	
	//Profiler Stop
	p.Stop()

	wg.Wait()
}
