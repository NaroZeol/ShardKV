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
)

type Config = common.Config
type ServerInfo = common.ServerInfo
type GroupInfo = common.GroupInfo

func main() {
	var id int
	var gid int
	var configPath string
	flag.IntVar(&id, "i", -1, "Server ID")
	flag.IntVar(&gid, "g", -1, "Group ID")
	flag.StringVar(&configPath, "c", "", "Config file path")
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

	// controllers
	ctrlers := common.MakeClientEnds(config.Ctrlers)

	// create persister
	persister := raft.MakePersister(gid, id)
	maxRaftState := 2048

	// start server
	sv := shardkv.StartServer(servers, id, persister, maxRaftState, gid, ctrlers, common.MakeEnd)
	rpc.Register(sv)

	// wait forever
	select {}
}
