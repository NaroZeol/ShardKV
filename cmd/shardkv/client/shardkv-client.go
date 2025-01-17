package main

import (
	"flag"
	"fmt"
	"log"

	"6.5840/cmd/common"
	"6.5840/shardkv"
)

type Config = common.Config
type ServerInfo = common.ServerInfo
type GroupInfo = common.GroupInfo

func main() {
	var configPath string
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig(configPath)

	// connect to shardctrler
	ctrlers := common.ConnectToServers(config.Ctrlers, map[int]bool{})
	log.Println("Connected to all controllers")

	ck := shardkv.MakeClerk(ctrlers, common.MakeEnd)

	// server loop
	for {
		var cmd string
		fmt.Print("> ")
		fmt.Scanln(&cmd)

		switch cmd {
		case "put":
			var key, value string
			fmt.Scanln(&key, &value)
			ck.Put(key, value)
		case "get":
			var key string
			fmt.Scanln(&key)
			value := ck.Get(key)
			fmt.Println(value)
		case "append":
			var key, value string
			fmt.Scanln(&key, &value)
			ck.Append(key, value)
		case "help":
			fmt.Println("Commands: put, get, append, help")
		default:
			fmt.Println("Unknown command")
		}
	}
}
