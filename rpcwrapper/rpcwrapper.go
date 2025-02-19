package rpcwrapper

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientEnd struct {
	rc   *grpc.ClientConn
	addr string
	port int
}

func MakeClient(addr string, port int) *ClientEnd {
	return &ClientEnd{nil, addr, port}
}

func (e *ClientEnd) GetConnection() *grpc.ClientConn {
	if e.rc == nil {
		conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", e.addr, e.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil
		}
		e.rc = conn
	}
	return e.rc
}

func (e *ClientEnd) Close() {
	if e.rc != nil {
		e.rc.Close()
	}
}

func (e *ClientEnd) GetAddrAndPort() string {
	return fmt.Sprintf("%s:%d", e.addr, e.port)
}
