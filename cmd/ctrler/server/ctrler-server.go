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
	"6.5840/shardctrler"
)

type ServerInfo struct {
	Id   int    `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type Config struct {
	NCtrler int          `json:"nctrler"`
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
	var configPath string
	flag.IntVar(&id, "i", -1, "Server ID")
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.Parse()

	// Load configuration
	config := LoadConfig(configPath)

	// start RPC server
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Ctrlers[id].Port))
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Printf("Server %d listening on port %d\n", id, config.Ctrlers[id].Port)
	go http.Serve(listener, nil)

	// connect to other controllers
	servers := make([]*labrpc.ClientEnd, config.NCtrler)
	wg := sync.WaitGroup{}
	for i := 0; i < config.NCtrler; i++ {
		if i == id {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", config.Ctrlers[i].Host, config.Ctrlers[i].Port))
				if err != nil {
					log.Printf("Failed to connect to server %d\n", i)
					time.Sleep(1 * time.Second)
				} else {
					servers[i] = labrpc.MakeClient(client, config.Ctrlers[i].Host, config.Ctrlers[i].Port)
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

	// make persister
	persister := raft.MakePersister()

	// start shardctrler
	ctrler := shardctrler.StartServer(servers, id, persister)
	rpc.Register(ctrler)

	select {}
}
