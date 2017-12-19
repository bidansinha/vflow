//: ----------------------------------------------------------------------------
//: Copyright (C) 2017 Verizon.  All Rights Reserved.
//: All Rights Reserved
//:
//: file:    ipfix.go
//: details: ipfix decoders handler
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

package main

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bidansinha/vflow/ipfix"
	"fmt"
	"log"
	"github.com/bidansinha/vflow/producer"
	"github.com/bidansinha/vflow/tcp_server"
)

// IPFIX represents IPFIX collector
type IPFIXTCP struct {
	port    int
	addr    string
	workers int
	stop    bool
	stats   IPFIXTCPStats
	pool    chan chan struct{}
}

// IPFIXUDPMsg represents IPFIX UDP data
type IPFIXTCPMsg struct {
	raddr *net.TCPAddr
	body  []byte
}

// IPFIXStats represents IPFIX stats
type IPFIXTCPStats struct {
	TCPQueue       int
	TCPMirrorQueue int
	MessageQueue   int
	TCPCount       uint64
	DecodedCount   uint64
	MQErrorCount   uint64
	Workers        int32
}

var (
	ipfixTCPCh         	  = make(chan IPFIXTCPMsg, 1000)
	ipfixTCPMCh           = make(chan IPFIXTCPMsg, 1000)
	ipfixTCPMQCh          = make(chan []byte, 1000)
	ipfixTCPMirrorEnabled bool

	// templates memory cache
	mCacheTCP ipfix.MemCache

	// ipfix udp payload pool
	ipfixTCPBuffer = &sync.Pool{
		New: func() interface{} {
			return make([]byte, opts.IPFIXTCPSize)
		},
	}
)

// NewIPFIX constructs IPFIX
func NewIPFIXTCP() *IPFIXTCP {
	return &IPFIXTCP{
		port:    opts.IPFIXTCPPort,
		workers: opts.IPFIXTCPWorkers,
		pool:    make(chan chan struct{}, maxWorkers),
	}
}

func (i *IPFIXTCP) run() {
	// exit if the ipfix is disabled
	if !opts.IPFIXTCPEnabled {
		logger.Println("ipfixTCP has been disabled")
		return
	}

	connectionString := fmt.Sprintf("%s:%d","0.0.0.0", i.port);
	
 	// TODO: Make the Options size accessable to TCP
 	//		 Thru the config `yaml:"ipfix-tcp-size"
	server := tcp_server.New(connectionString, opts.IPFIXTCPSize); 
	
	server.OnNewClient(func(c *tcp_server.Client) {
		// new client connected
		// lets send some message
		log.Println(c.Conn().RemoteAddr().String(), " open ");
	})

	server.OnNewMessage(func(c *tcp_server.Client, buf [] byte, size int) {
		raddr, err := net.ResolveTCPAddr("tcp",c.Conn().RemoteAddr().String());
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Incoming Size :", size)
		b := ipfixBuffer.Get().([]byte)

		/***********************************************
 		 * Need to find a better way to avoid copying.
		 **********************************************/
		copy(b ,buf[:size])
		atomic.AddUint64(&i.stats.TCPCount, 1)
		ipfixTCPCh <- IPFIXTCPMsg{raddr,  b[:size]}
	})

	server.OnClientConnectionClosed(func(c *tcp_server.Client, err error) {
		log.Println(c.Conn().RemoteAddr().String(), " closed ");
	})

	atomic.AddInt32(&i.stats.Workers, int32(i.workers))
	for n := 0; n < i.workers; n++ {
		go func() {
			wQuit := make(chan struct{})
			i.pool <- wQuit
			i.ipfixTCPWorker(wQuit)
		}()
	}

	logger.Printf("ipfix TCP is running (TCP: listening on [::]:%d workers#: %d)", i.port, i.workers)

	mCache = ipfix.GetCache(opts.IPFIXTplCacheFile)
	go ipfix.RPC(mCache, &ipfix.RPCConfig{
		Enabled: opts.IPFIXRPCEnabled,
		Logger:  logger,
	})

	//go mirrorIPFIXDispatcher(ipfixMCh)
	go func() {
		p := producer.NewProducer(opts.MQName)

		p.MQConfigFile = opts.MQConfigFile
		p.MQErrorCount = &i.stats.MQErrorCount
		p.Logger = logger
		p.Chan = ipfixMQCh
		p.Topic = opts.IPFIXTopic

		if err := p.Run(); err != nil {
			/****************************************************
			 * TODO [bsinha] : Make a Option to selectively allow
			 * Kafka. This will allow us to choose sampling data 
			 * for the pipeline such that we only enable a few
			 * servers to stream to Kafka, not all.
			 ****************************************************/
			logger.Println(err)
			logger.Println("Continuing without producer.")
		}
	}()

	go func() {
		
		var (
			msg []byte
			ok	bool
			)

		for {
			msg, ok = <-ipfixMQCh
			if !ok {
				break
			}
			logger.Println("S3 Write Block:", string(msg))

			/*fileout, err := os.Create("output.txt")
			if err != nil {
		        log.Fatal(err)
		    
		    n = len(msg)
		    if _, err := fo.Write(msg[:n]); err != nil {
            	log.Fatal(err)
        	}

        	if err := fo.Close(); err != nil {
	            log.Fatal(err)
	        }*/
		}
	}()

	// External IP Source IP/ dump
	go func() {
		if !opts.DynWorkers {
			logger.Println("IPFIX dynamic worker disabled")
			return
		}

		i.dynTCPWorkers()
	}()

	server.Listen()
}


func (i *IPFIXTCP) shutdown() {
	// exit if the ipfix is disabled
	if !opts.IPFIXTCPEnabled {
		logger.Println("ipfix TCP disabled")
		return
	}

	// stop reading from UDP listener
	i.stop = true
	logger.Println("stopping ipfix tcp service gracefully ...")
	time.Sleep(1 * time.Second)

	// dump the templates to storage
	if err := mCache.Dump(opts.IPFIXTplCacheFile); err != nil {
		logger.Println("couldn't not dump template", err)
	}

	// logging and close UDP channel
	logger.Println("ipfix tcp has been shutdown")
	close(ipfixTCPCh)
	close(ipfixTCPMCh)
	close(ipfixTCPMQCh)

}

func (i *IPFIXTCP) ipfixTCPWorker (wQuit chan struct{}) {
	var (
		decodedMsg *ipfix.Message
		msg        = IPFIXTCPMsg{body: ipfixTCPBuffer.Get().([]byte)}
		buf        = new(bytes.Buffer)
		err        error
		ok         bool
		b          []byte
	)

LOOP:
	for {
		ipfixTCPBuffer.Put(msg.body[:opts.IPFIXTCPSize])
		buf.Reset()

		select {
		case <-wQuit:
			break LOOP
		case msg, ok = <-ipfixTCPCh:
			if !ok {
				break LOOP
			}
		}

		if opts.Verbose {
			logger.Printf("rcvd ipfix data from: %s, size: %d bytes",
				msg.raddr, len(msg.body))
		}
		logger.Println("Message Body Size :",  len(msg.body))
		//Mirroring option removed
		d := ipfix.NewDecoder(msg.raddr.IP, msg.body)
		if decodedMsg, err = d.Decode(mCache); err != nil {
			logger.Println("Error 11 " ,err)
			// in case ipfix message header couldn't decode
			if decodedMsg == nil {
				logger.Println("Could not be decoded.")
				continue
			}
		}

		atomic.AddUint64(&i.stats.DecodedCount, 1)
		if len(decodedMsg.DataSets) > 0 {
			logger.Println("Message contains Datasets.")
		} else {
			logger.Println("Message Does not contain Datasets.")

		}	
		
		b, err = decodedMsg.JSONMarshal(buf)
		/**************************************
		 * TODO [bsinha] Add a S3 Producer Here
		 *************************************/

		if err != nil {
			logger.Println(err)
			continue
		}

		select {
		case ipfixMQCh <- append([]byte{}, b...):
		default:
		}
		//logger.Println(string(b))
		if opts.Verbose {
			logger.Println(string(b))
		}
	}
}

func (i *IPFIXTCP) status() *IPFIXTCPStats {
	return &IPFIXTCPStats{
		TCPQueue:       len(ipfixTCPCh),
		TCPMirrorQueue: len(ipfixTCPMCh),
		MessageQueue:   len(ipfixTCPMQCh),
		TCPCount:       atomic.LoadUint64(&i.stats.TCPCount ),
		DecodedCount:   atomic.LoadUint64(&i.stats.DecodedCount),
		MQErrorCount:   atomic.LoadUint64(&i.stats.MQErrorCount),
		Workers:        atomic.LoadInt32(&i.stats.Workers),
	}
}

func (i *IPFIXTCP) dynTCPWorkers() {
	var load, nSeq, newWorkers, workers, n int

	tick := time.Tick(120 * time.Second)

	for {
		<-tick
		load = 0

		for n = 0; n < 30; n++ {
			time.Sleep(1 * time.Second)
			load += len(ipfixTCPCh)
		}

		if load > 15 {

			switch {
			case load > 300:
				newWorkers = 100
			case load > 200:
				newWorkers = 60
			case load > 100:
				newWorkers = 40
			default:
				newWorkers = 30
			}

			workers = int(atomic.LoadInt32(&i.stats.Workers))
			if workers+newWorkers > maxWorkers {
				logger.Println("ipfix :: max out workers")
				continue
			}

			for n = 0; n < newWorkers; n++ {
				go func() {
					atomic.AddInt32(&i.stats.Workers, 1)
					wQuit := make(chan struct{})
					i.pool <- wQuit
					i.ipfixTCPWorker(wQuit)
				}()
			}

		}

		if load == 0 {
			nSeq++
		} else {
			nSeq = 0
			continue
		}

		if nSeq > 15 {
			for n = 0; n < 10; n++ {
				if len(i.pool) > i.workers {
					atomic.AddInt32(&i.stats.Workers, -1)
					wQuit := <-i.pool
					close(wQuit)
				}
			}

			nSeq = 0
		}
	}
}
