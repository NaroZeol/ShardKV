package common

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
	NCtrler int          `json:"nctrler"`
	Groups  []GroupInfo  `json:"groups"`
	Ctrlers []ServerInfo `json:"ctrlers"`
}
