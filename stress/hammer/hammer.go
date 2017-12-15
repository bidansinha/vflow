//: ----------------------------------------------------------------------------
//: Copyright (C) 2017 Verizon.  All Rights Reserved.
//: All Rights Reserved
//:
//: file:    hammer.go
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

package hammer

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bidansinha/vflow/mirror"
)

const (
	dataType = iota
	templateType
	templateOptType
	sFlowDataType
)

// Packet represents generated packet
type Packet struct {
	payload []byte
	length  int
}

// IPFIX represents IPFIX packet generator
type IPFIX struct {
	conn  mirror.Conn
	ch    chan Packet
	srcs  []net.IP
	vflow net.IP

	MaxRouter int
	Tick      time.Duration
	Port      int
	RateLimit int
}

// IPFIX represents IPFIX packet generator
type IPFIXTCP struct {
	connectionString  string
	ch    chan Packet
	srcs  []net.IP
	vflow net.IP

	MaxRouter int
	Tick      time.Duration
	Port      int
	RateLimit int
}

// SFlow represents SFlow packet generator
type SFlow struct {
	conn  mirror.Conn
	ch    chan Packet
	srcs  []net.IP
	vflow net.IP

	MaxRouter int
	Port      int
	RateLimit int
}

// NewIPFIX constructs new IPFIX packet generator
func NewIPFIX(raddr net.IP) (*IPFIX, error) {

	conn, err := mirror.NewRawConn(raddr)
	if err != nil {
		return nil, err
	}

	return &IPFIX{
		conn:      conn,
		ch:        make(chan Packet, 10000),
		vflow:     raddr,
		MaxRouter: 10,
		RateLimit: 25 * 10e3,
	}, nil
}


// NewIPFIX constructs new IPFIX packet generator
func NewIPFIXTCP(domain string, port int, raddr net.IP) (*IPFIXTCP, error) {

	connectionString := fmt.Sprintf("%s:%d",domain,port);


	return &IPFIXTCP{
		connectionString:      connectionString,
		ch:        make(chan Packet, 10000),
		vflow:     raddr,
		MaxRouter: 10,
		RateLimit: 25 * 10e3,
	}, nil
}


func startClient(connectionString string, callback tcp.CallBack, protocol tcp.Protocol, scount int , mcount int, msg[] byte) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", connectionString)
	if err != nil {
		panic(err)
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		panic(err)
	}
	tc := tcp.NewTCPConn(conn, callback, protocol)
	log.Println(tc.Serve())
	i := scount
	for {
		if tc.IsClosed() {
			break
		}
		//msg1 := fmt.Sprintf("hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello %d", i)
		log.Println("client send: ", msg)
		tc.AsyncWritePacket(&tcp.DefaultPacket{Type: 1, Body: msg})
		i++
		time.Sleep(time.Second)
		if((i) > mcount) {
			//break
			tc.Close()
			break
		}

	}
}

type callback struct{}

func (c *callback) OnMessage(conn *tcp.TCPConn, p tcp.Packet) {
	log.Println("server receive:", string(p.Bytes()[1:]))
}

func (c *callback) OnConnected(conn *tcp.TCPConn) {
	log.Println("new conn:", conn.GetRemoteAddr().String())
}

func (c *callback) OnDisconnected(conn *tcp.TCPConn) {
	log.Printf("%s disconnected\n", conn.GetRemoteIPAddress())
}

func (c *callback) OnError(err error) {
	log.Println(err)
}

// Run starts IPFIX simulator - attacking
func (i *IPFIXTCP) Run() {
	var wg sync.WaitGroup

	for j := 1; j < i.MaxRouter; j++ {
		i.srcs = append(i.srcs, net.ParseIP(fmt.Sprintf("192.168.1.%d", j)))
	}

	wg.Add(1)
	go func() {
		var p Packet
		defer wg.Done()
		for {
			p = <-i.ch

			protocol := &tcp.DefaultProtocol{}
			protocol.SetMaxPacketSize(2000)
			//startServer(&callback{}, protocol)
			log.Println("lenght ", p.length);
			startClient(i.connectionString, &callback{}, protocol, 0, 0, p.payload[:p.length])
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.sendTemplateTCP()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.sendTemplateOptTCP()
	}()

	time.Sleep(1 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.sendDataTCP()
	}()

	wg.Wait()
}


// Run starts IPFIX simulator - attacking
func (i *IPFIX) Run() {
	var wg sync.WaitGroup

	for j := 1; j < i.MaxRouter; j++ {
		i.srcs = append(i.srcs, net.ParseIP(fmt.Sprintf("192.168.1.%d", j)))
	}

	wg.Add(1)
	go func() {
		var p Packet
		defer wg.Done()
		for {
			p = <-i.ch
			i.conn.Send(p.payload[:p.length])
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.sendTemplate()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.sendTemplateOpt()
	}()

	time.Sleep(1 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.sendData()
	}()

	wg.Wait()
}

func (i *IPFIXTCP) sendDataTCP() {
	var (
		throttle <-chan time.Time
		packets  = i.genPacketsTCP(dataType)
	)

	if i.RateLimit > 0 {
		throttle = time.Tick(time.Duration(1e6/(i.RateLimit)) * time.Microsecond)
	}

	for {
		for j := range packets {
			<-throttle
			i.ch <- packets[j]
		}
	}
}
func (i *IPFIXTCP) sendTemplateTCP() {
	c := time.Tick(i.Tick)
	packets := i.genPacketsTCP(templateType)

	for j := range packets {
		i.ch <- packets[j]
	}

	for range c {
		for j := range packets {
			i.ch <- packets[j]
		}
	}
}

func (i *IPFIXTCP) sendTemplateOptTCP() {
	c := time.Tick(i.Tick)
	packets := i.genPacketsTCP(templateOptType)

	for j := range packets {
		i.ch <- packets[j]
	}

	for range c {
		for j := range packets {
			i.ch <- packets[j]
		}
	}
}

func (i *IPFIXTCP) genPacketsTCP(typ int) []Packet {
	var (
		packets []Packet
		samples [][]byte
	)

	ipHLen := mirror.IPv4HLen
	udp := mirror.UDP{55117, i.Port, 0, 0}
	udpHdr := udp.Marshal()

	ip := mirror.NewIPv4HeaderTpl(mirror.UDPProto)
	ipHdr := ip.Marshal()

	switch typ {
	case dataType:
		samples = ipfixDataSamples
	case templateType:
		samples = ipfixTemplates
	case templateOptType:
		samples = ipfixTemplatesOpt
	case sFlowDataType:
		samples = sFlowDataSamples
	}

	for j := 0; j < len(samples); j++ {
		for _, src := range i.srcs {
			data := samples[j]
			payload := make([]byte, 1500)

			udp.SetLen(udpHdr, len(data))

			ip.SetAddrs(ipHdr, src, i.vflow)

			copy(payload[0:ipHLen], ipHdr)
			copy(payload[ipHLen:ipHLen+8], udpHdr)
			copy(payload[ipHLen+8:], data)

			packets = append(packets, Packet{
				payload: payload,
				length:  ipHLen + 8 + len(data),
			})

		}
	}

	return packets
}

func (i *IPFIX) sendData() {
	var (
		throttle <-chan time.Time
		packets  = i.genPackets(dataType)
	)

	if i.RateLimit > 0 {
		throttle = time.Tick(time.Duration(1e6/(i.RateLimit)) * time.Microsecond)
	}

	for {
		for j := range packets {
			<-throttle
			i.ch <- packets[j]
		}
	}
}

func (i *IPFIX) sendTemplate() {
	c := time.Tick(i.Tick)
	packets := i.genPackets(templateType)

	for j := range packets {
		i.ch <- packets[j]
	}

	for range c {
		for j := range packets {
			i.ch <- packets[j]
		}
	}
}

func (i *IPFIX) sendTemplateOpt() {
	c := time.Tick(i.Tick)
	packets := i.genPackets(templateOptType)

	for j := range packets {
		i.ch <- packets[j]
	}

	for range c {
		for j := range packets {
			i.ch <- packets[j]
		}
	}
}

func (i *IPFIX) genPackets(typ int) []Packet {
	var (
		packets []Packet
		samples [][]byte
	)

	ipHLen := mirror.IPv4HLen
	udp := mirror.UDP{55117, i.Port, 0, 0}
	udpHdr := udp.Marshal()

	ip := mirror.NewIPv4HeaderTpl(mirror.UDPProto)
	ipHdr := ip.Marshal()

	switch typ {
	case dataType:
		samples = ipfixDataSamples
	case templateType:
		samples = ipfixTemplates
	case templateOptType:
		samples = ipfixTemplatesOpt
	case sFlowDataType:
		samples = sFlowDataSamples
	}

	for j := 0; j < len(samples); j++ {
		for _, src := range i.srcs {
			data := samples[j]
			payload := make([]byte, 1500)

			udp.SetLen(udpHdr, len(data))

			ip.SetAddrs(ipHdr, src, i.vflow)

			copy(payload[0:ipHLen], ipHdr)
			copy(payload[ipHLen:ipHLen+8], udpHdr)
			copy(payload[ipHLen+8:], data)

			packets = append(packets, Packet{
				payload: payload,
				length:  ipHLen + 8 + len(data),
			})

		}
	}

	return packets
}

// NewSFlow constructs SFlow packet generator
func NewSFlow(raddr net.IP) (*SFlow, error) {

	conn, err := mirror.NewRawConn(raddr)
	if err != nil {
		return nil, err
	}

	return &SFlow{
		conn:      conn,
		ch:        make(chan Packet, 10000),
		vflow:     raddr,
		MaxRouter: 10,
		RateLimit: 25 * 10e3,
	}, nil
}

// Run starts sFlow simulator - attacking
func (s *SFlow) Run() {
	var wg sync.WaitGroup

	for j := 1; j < s.MaxRouter; j++ {
		s.srcs = append(s.srcs, net.ParseIP(fmt.Sprintf("192.168.1.%d", j)))
	}

	wg.Add(1)
	go func() {
		var p Packet
		defer wg.Done()
		for {
			p = <-s.ch
			s.conn.Send(p.payload[:p.length])
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.sendData()
	}()

	wg.Wait()
}

func (s *SFlow) genPackets() []Packet {
	var packets []Packet
	ipHLen := mirror.IPv4HLen
	udp := mirror.UDP{55117, s.Port, 0, 0}
	udpHdr := udp.Marshal()

	ip := mirror.NewIPv4HeaderTpl(mirror.UDPProto)
	ipHdr := ip.Marshal()

	for j := 0; j < len(sFlowDataSamples); j++ {
		for _, src := range s.srcs {
			data := sFlowDataSamples[j]
			payload := make([]byte, 1500)

			udp.SetLen(udpHdr, len(data))

			ip.SetAddrs(ipHdr, src, s.vflow)

			copy(payload[0:ipHLen], ipHdr)
			copy(payload[ipHLen:ipHLen+8], udpHdr)
			copy(payload[ipHLen+8:], data)

			packets = append(packets, Packet{
				payload: payload,
				length:  ipHLen + 8 + len(data),
			})

		}
	}

	return packets
}

func (s *SFlow) sendData() {
	var (
		throttle <-chan time.Time
		packets  = s.genPackets()
	)

	if s.RateLimit > 0 {
		throttle = time.Tick(time.Duration(1e6/(s.RateLimit)) * time.Microsecond)
	}

	for {
		for j := range packets {
			<-throttle
			s.ch <- packets[j]
		}
	}
}
