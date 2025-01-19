package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"6.5840/cmd/common"
	"6.5840/shardkv"
)

type Config = common.Config
type ServerInfo = common.ServerInfo
type GroupInfo = common.GroupInfo

func handleInput() (action, key, value string) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()

	line := strings.TrimSpace(scanner.Text())
	parts := strings.Fields(line)

	if len(parts) == 1 {
		return parts[0], "", ""
	} else if len(parts) == 2 {
		return parts[0], parts[1], ""
	} else if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}

	return "", "", ""
}

func main() {
	var configPath string
	var interactive bool
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.BoolVar(&interactive, "i", false, "Interactive mode")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig(configPath)

	// connect to shardctrler
	ctrlers := common.MakeClientEnds(config.Ctrlers)

	ck := shardkv.MakeClerk(ctrlers, common.MakeEnd)

	// server loop
	for {
		if interactive {
			fmt.Print("shardkv> ")
		}
		cmd, key, value := handleInput()

		switch cmd {
		case "put":
			ck.Put(key, value)
		case "get":
			value := ck.Get(key)
			fmt.Println(value)
		case "append":
			ck.Append(key, value)
		case "help":
			fmt.Println("Commands: put, get, append, help")
		default:
			fmt.Println("Unknown command")
		}
	}
}
