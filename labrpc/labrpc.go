package labrpc

import (
	"fmt"
	// "log"
	"net/rpc"
	"time"
)

type ClientEnd struct {
	rc   *rpc.Client
	addr string
	port int
}

func MakeClient(rc *rpc.Client, addr string, port int) *ClientEnd {
	return &ClientEnd{rc, addr, port}
}

// send an RPC, wait for the reply.
// the return value indicates success; false means that
// no reply was received from the server.
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	done := make(chan error, 1)
	go func() {
		done <- e.rc.Call(svcMeth, args, reply)
	}()

	// log.Print("Call ", svcMeth, " to ", e.addr, ":", e.port)

	for {
		select {
		case err := <-done:
			if err != nil { // try to reconnect
				c, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", e.addr, e.port))
				if err != nil { // reconnect failed
					// log.Println("Failed to reconnect to ", e.addr, ":", e.port)
					return false
				}
				e.rc = c
			} else {
				// log.Println("Call ", svcMeth, " to ", e.addr, ":", e.port, " succeeded")
				return true
			}
		case <-time.After(1 * time.Second):
			// log.Println("Call ", svcMeth, " to ", e.addr, ":", e.port, " timed out")
			return false
		}
	}
}
