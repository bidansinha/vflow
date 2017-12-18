package tcp_server

import (
	"log"
	"net"
	"io"
	//"sync"
)

// Client holds info about connection
type Client struct {
	conn   net.Conn
	Server *server
}

// TCP server
type server struct {
	address                  string // Address to open connection: localhost:9999
	poolSize				 int
	onNewClientCallback      func(c *Client)
	onClientConnectionClosed func(c *Client, err error)
	onNewMessage			 func(c *Client, byte []byte, size int)
}

// Read client data from channel
func (c *Client) listen() {
	/*var (
		ipfixTCPBuffer = &sync.Pool{
			New: func() interface{} {
				return make([]byte, c.Server.poolSize)
			},
		}
	)*/

	tmp := make([]byte, c.Server.poolSize)

	defer c.Close();

	for {
		n, err := c.conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				log.Println("read error:", err)
				c.Close();
				return
			}
			log.Println(" error breaking ", err)
			c.Close()
			break
		} else {
			// Received Bytes
			c.Server.onNewMessage(c, tmp, n)
		}
	}
}

// Send text message to client
func (c *Client) Send(message string) error {
	_, err := c.conn.Write([]byte(message))
	return err
}

// Send bytes to client
func (c *Client) SendBytes(b []byte) error {
	_, err := c.conn.Write(b)
	return err
}

func (c *Client) Conn() net.Conn {
	return c.conn
}

func (c *Client) Close() error {
	c.Server.onClientConnectionClosed(c, nil)
	return c.conn.Close()
}

// Called right after server starts listening new client
func (s *server) OnNewClient(callback func(c *Client)) {
	s.onNewClientCallback = callback
}

// Called right after connection closed
func (s *server) OnClientConnectionClosed(callback func(c *Client, err error)) {
	s.onClientConnectionClosed = callback
}

// Called when Client receives new message
func (s *server) OnNewMessage(callback func(c *Client, bytes [] byte, size int)) {
	s.onNewMessage = callback
}

// Start network server
func (s *server) Listen() {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		log.Fatal("Error starting TCP server.")
	}
	defer listener.Close()

	for {
		conn, _ := listener.Accept()
		client := &Client{
			conn:   conn,
			Server: s,
		}
		go client.listen()
		s.onNewClientCallback(client)
	}
}

// Creates new tcp server instance
func New(address string, poolSize int) *server {
	log.Println("Creating server with address ", address, " and poolSize ", poolSize)
	server := &server{
		address: address,
		poolSize : poolSize,
	}

	server.OnNewClient(func(c *Client) {})
	server.OnClientConnectionClosed(func(c *Client, err error) {})
	server.OnNewMessage(func(c *Client, byte[] byte, size int) {})

	return server
}
