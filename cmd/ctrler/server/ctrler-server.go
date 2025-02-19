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

	"google.golang.org/grpc"
)

type Config = common.Config
type ServerInfo = common.ServerInfo

func main() {
	var id int
	var configPath string
	var maxraftstate int
	flag.IntVar(&id, "i", -1, "Server ID")
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.IntVar(&maxraftstate, "m", 8192, "Max Raft state size(bytes)")
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

	// connect to raft servers
	raftServers := common.MakeRaftEnds(config.Ctrlers)

	// make persister
	persister := raft.MakePersister(-1, id) // -1 to shardctrler

	// grpc init
	raftListener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Ctrlers[id].RaftPort))
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Printf("Raft server %d listening on port %d\n", id, config.Ctrlers[id].RaftPort)
	grpcServer := grpc.NewServer()

	// start shardctrler
	ctrler := shardctrler.StartServer(grpcServer, servers, raftServers, id, persister, maxraftstate)
	rpc.Register(ctrler)
	go grpcServer.Serve(raftListener)

	select {}
}
