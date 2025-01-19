package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"6.5840/cmd/common"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Config = common.Config
type ServerInfo = common.ServerInfo

func main() {
	var id int
	var configPath string
	flag.IntVar(&id, "i", -1, "Server ID")
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig(configPath)

	// start RPC server
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Ctrlers[id].Port))
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Printf("Server %d listening on port %d\n", id, config.Ctrlers[id].Port)
	go http.Serve(listener, nil)

	// connect to other controllers
	servers := common.MakeClientEnds(config.Ctrlers)

	// make persister
	persister := raft.MakePersister(-1, id) // -1 to shardctrler

	// start shardctrler
	ctrler := shardctrler.StartServer(servers, id, persister)
	rpc.Register(ctrler)

	select {}
}
