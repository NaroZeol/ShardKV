package common

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"

	"6.5840/labrpc"
)

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

func ConnectToServers(servers []ServerInfo, excluded map[int]bool) []*labrpc.ClientEnd {
	clientends := make([]*labrpc.ClientEnd, len(servers))

	wg := sync.WaitGroup{}
	for i := 0; i < len(servers); i++ {
		if excluded[i] {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", servers[i].Host, servers[i].Port))
				if err != nil {
					log.Printf("Failed to connect to %s:%d\n", servers[i].Host, servers[i].Port)
					time.Sleep(1 * time.Second)
				} else {
					clientends[i] = labrpc.MakeClient(client, servers[i].Host, servers[i].Port)
					log.Printf("Connected to %s:%d\n", servers[i].Host, servers[i].Port)
					break
				}
			}
		}(i)
	}
	wg.Wait()

	return clientends
}
