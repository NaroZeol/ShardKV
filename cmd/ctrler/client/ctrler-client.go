package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"6.5840/cmd/common"
	"6.5840/shardctrler"
)

type Config = common.Config
type ServerInfo = common.ServerInfo

func ReadJoinInfo() map[int][]string {
	servers := make(map[int][]string)

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
	var interactive bool
	var verbose bool
	flag.StringVar(&configPath, "c", "", "Config file path")
	flag.BoolVar(&interactive, "i", false, "Interactive mode")
	flag.BoolVar(&verbose, "v", false, "Verbose mode")
	flag.Parse()

	// Load configuration
	config := common.LoadConfig(configPath)

	// connect to shardctrler
	ctrlers := common.MakeClientEnds(config.Ctrlers)

	ck := shardctrler.MakeClerk(ctrlers, verbose)

	// server loop
	for {
		var cmd string
		if interactive {
			fmt.Print("ctrler> ")
		}
		fmt.Scan(&cmd)

		switch cmd {
		case "join":
			if interactive {
				fmt.Println("<format>: gid server1 server2 ... end with empty line")
			}
			servers := ReadJoinInfo()
			ck.Join(servers)
		case "leave":
			if interactive {
				fmt.Println("<format>: gid1 gid2 ... end with empty line")
			}
			gids := ReadLeaveInfo()
			ck.Leave(gids)
		case "query":
			var num int
			if interactive {
				fmt.Print("Num: ")
			}
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
