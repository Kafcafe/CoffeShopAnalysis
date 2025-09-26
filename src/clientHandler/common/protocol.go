package clientHandler

import "net"

type Protocol struct {
	conn net.Conn
}

func NewProtocol(conn net.Conn) *Protocol {
	return &Protocol{
		conn: conn,
	}
}
