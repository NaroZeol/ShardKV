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
	"6.5840/shardkv"
	"google.golang.org/grpc"
)

type Config = common.Config
type ServerInfo = common.ServerInfo
type GroupInfo = common.GroupInfo

func main() {
	var id int
	var gid int
	var configPath string
	var maxraftstate int
	flag.IntVar(&id, "i", -1, "Server ID")
	flag.IntVar(&gid, "g", -1, "Group ID")
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.IntVar(&maxraftstate, "m", 8192, "Max Raft state size(bytes)")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig(configPath)

	// start RPC server
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Groups[gid].Servers[id].Port))
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Printf("Server %d listening on port %d\n", id, config.Groups[gid].Servers[id].Port)
	go http.Serve(listener, nil)

	// servers
	servers := common.MakeClientEnds(config.Groups[gid].Servers)

	// raft servers
	raftServers := common.MakeRaftEnds(config.Groups[gid].Servers)

	// controllers
	ctrlers := common.MakeClientEnds(config.Ctrlers)

	// create persister
	persister := raft.MakePersister(gid, id)

	// gRPC init
	raftListener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Groups[gid].Servers[id].RaftPort))
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Printf("Raft server %d listening on port %d\n", id, config.Groups[gid].Servers[id].RaftPort)
	grpcServer := grpc.NewServer()

	// start server
	sv := shardkv.StartServer(grpcServer, servers, raftServers, id, persister, maxraftstate, gid, ctrlers, common.MakeEnd)
	rpc.Register(sv)
	go grpcServer.Serve(raftListener)

	// wait forever
	select {}
}
