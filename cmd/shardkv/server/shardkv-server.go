package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardkv"
)

type ServerInfo struct {
	Id   int    `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type GroupInfo struct {
	Gid     int          `json:"gid"`
	Servers []ServerInfo `json:"servers"`
}

type Config struct {
	NServer int          `json:"nserver"`
	Groups  []GroupInfo  `json:"groups"`
	Ctrlers []ServerInfo `json:"ctrlers"`
}

func LoadConfig(path string) Config {
	config := Config{}

	file, err := os.Open(path)
	if err != nil {
		log.Fatal("Error opening config file:", err)
	}

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		log.Fatal("Error decoding config file:", err)
	}
	file.Close()

	return config
}

func main() {
	var id int
	var gid int
	var configPath string
	flag.IntVar(&id, "i", -1, "Server ID")
	flag.IntVar(&gid, "g", -1, "Group ID")
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.Parse()

	// Load configuration
	config := LoadConfig(configPath)

	thisGroup := config.Groups[gid]

	// start RPC server
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", thisGroup.Servers[id].Port))
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Printf("Server %d listening on port %d\n", id, thisGroup.Servers[id].Port)
	go http.Serve(listener, nil)

	// connect to other servers
	servers := make([]*labrpc.ClientEnd, config.NServer)
	wg := sync.WaitGroup{}
	for i := 0; i < config.NServer; i++ {
		if i == id {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", thisGroup.Servers[i].Host, thisGroup.Servers[i].Port))
				if err != nil {
					log.Printf("Failed to connect to server %d\n", i)
					time.Sleep(1 * time.Second)
				} else {
					servers[i] = labrpc.MakeClient(client, thisGroup.Servers[i].Host, thisGroup.Servers[i].Port)
					log.Printf("Connected to server %d\n", i)
					break
				}
			}
		}(i)
	}

	if err != nil {
		log.Fatal("Serve error:", err)
	}

	wg.Wait()
	log.Println("All servers connected")

	// connect to controllers
	ctrlers := make([]*labrpc.ClientEnd, len(config.Ctrlers))
	for i := 0; i < len(config.Ctrlers); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", config.Ctrlers[i].Host, config.Ctrlers[i].Port))
				if err != nil {
					log.Printf("Failed to connect to controller %d\n", i)
					time.Sleep(1 * time.Second)
				} else {
					ctrlers[i] = labrpc.MakeClient(client, config.Ctrlers[i].Host, config.Ctrlers[i].Port)
					log.Printf("Connected to controller %d\n", i)
					break
				}
			}
		}(i)
	}
	wg.Wait()
	log.Println("All controllers connected")

	// create persister
	persister := raft.MakePersister()
	maxRaftState := 2048

	// start server
	sv := shardkv.StartServer(servers, id, persister, maxRaftState, gid, ctrlers,
		func(addr string) *labrpc.ClientEnd {
			return &labrpc.ClientEnd{} // TODO: implement
		})
	rpc.Register(sv)

	// wait forever
	select {}
}
