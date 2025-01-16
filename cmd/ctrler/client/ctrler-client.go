package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"6.5840/labrpc"
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

func ReadJoinInfo() map[int][]string {
	servers := make(map[int][]string)
	fmt.Println("<format>: gid server1 server2 ... end with empty line")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" { // empty line, end of input
			break
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			fmt.Println("Wrong format, each line should have at least one gid and one server address")
			continue
		}

		gid, err := strconv.Atoi(parts[0])
		if err != nil {
			fmt.Println("Wrong format, gid should be an integer")
			continue
		}

		serverList := parts[1:]

		servers[gid] = append(servers[gid], serverList...)
	}

	return servers
}

func ReadLeaveInfo() []int {
	var gids []int
	fmt.Println("<format>: gid1 gid2 ... end with empty line")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" { // empty line, end of input
			break
		}

		parts := strings.Fields(line)
		for _, part := range parts {
			gid, err := strconv.Atoi(part)
			if err != nil {
				fmt.Println("Wrong format, gid should be an integer")
				continue
			}
			gids = append(gids, gid)
		}
	}

	return gids
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.Parse()

	// Load configuration
	config := LoadConfig(configPath)

	// connect to shardctrler
	ctrlers := make([]*labrpc.ClientEnd, config.NCtrler)
	wg := sync.WaitGroup{}
	for i := 0; i < config.NCtrler; i++ {
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
	log.Println("Connected to all controllers")

	ck := shardctrler.MakeClerk(ctrlers)

	// server loop
	for {
		var cmd string
		fmt.Print("> ")
		fmt.Scan(&cmd)

		switch cmd {
		case "join":
			servers := ReadJoinInfo()
			ck.Join(servers)
		case "leave":
			gids := ReadLeaveInfo()
			ck.Leave(gids)
		case "query":
			var num int
			fmt.Print("Num: ")
			fmt.Scan(&num)
			cfg := ck.Query(num)
			fmt.Println(cfg)
		case "help":
			fmt.Println("Commands: join, leave, query, help")
		default:
			fmt.Println("Unknown command")
		}
	}
}
