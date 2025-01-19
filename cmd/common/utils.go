package common

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"

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

func MakeClientEnds(servers []ServerInfo) []*labrpc.ClientEnd {
	clientEnds := make([]*labrpc.ClientEnd, len(servers))
	for i, server := range servers {
		clientEnds[i] = labrpc.MakeClient(server.Host, server.Port)
	}
	return clientEnds
}

func MakeEnd(name string) (clientend *labrpc.ClientEnd, err error) {
	fileds := strings.Split(name, ":")

	host := fileds[0]
	port, err := strconv.Atoi(fileds[1])
	if err != nil {
		log.Println("Wrong format, port should be an integer")
	}

	return labrpc.MakeClient(host, port), nil
}
