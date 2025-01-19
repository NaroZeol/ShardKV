package common

type ServerInfo struct {
	Id   int    `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type GroupInfo struct {
	Gid     int          `json:"gid"`
	NServer int          `json:"nserver"`
	Servers []ServerInfo `json:"servers"`
}

type Config struct {
	NCtrler int          `json:"nctrler"`
	Ctrlers []ServerInfo `json:"ctrlers"`
	Groups  []GroupInfo  `json:"groups"`
}
