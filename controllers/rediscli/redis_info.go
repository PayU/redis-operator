package rediscli

import (
	"strings"
)

// https://redis.io/commands/info
type RedisInfo struct {
	Server      map[string]string
	Clients     map[string]string
	Memory      map[string]string
	Persistence map[string]string
	Stats       map[string]string
	Replication map[string]string
	CPU         map[string]string
	Cluster     map[string]string
	Modules     map[string]string
	Keyspace    map[string]string
}

// https://redis.io/commands/cluster-info
type RedisClusterInfo map[string]string

// RedisClusterNodes command: https://redis.io/commands/cluster-nodes
/*
example:
a917cda34af85862ba032a39fe0f9e7391dc7320 10.244.3.3:6379@16379 slave 8bac63eb45663020b4cc96b0cecdf233b54aa4be 0 1604407449303 3 connected
1e8af559bebe994b3a7a14b07f0cae06570fc6f4 10.244.7.2:6379@16379 slave 3729be2f3ba0fa056b71c275d587d43d23c9b593 0 1604407450000 1 connected
8bac63eb45663020b4cc96b0cecdf233b54aa4be 10.244.6.3:6379@16379 myself,master - 0 1604407448000 3 connected 10923-16383
ed35cf6b591e1463b43bb67d0639b9a78f0ab6c6 10.244.9.2:6379@16379 slave a58a2cac1ab0dab0add6a0f541f426b2504e9c13 0 1604407450110 2 connected
fabfc4389ead75f86bc1190ce1a990bfeee74ea9 10.244.8.2:6379@16379 slave a58a2cac1ab0dab0add6a0f541f426b2504e9c13 0 1604407450511 2 connected
3735d298fee93f2f4062c88b80bee1d51331bde0 10.244.1.2:6379@16379 slave 3729be2f3ba0fa056b71c275d587d43d23c9b593 0 1604407450511 1 connected
3729be2f3ba0fa056b71c275d587d43d23c9b593 10.244.5.2:6379@16379 master - 0 1604407449000 1 connected 0-5460
a58a2cac1ab0dab0add6a0f541f426b2504e9c13 10.244.2.2:6379@16379 master - 0 1604407450310 2 connected 5461-10922
d980857e202c9fc237b27616994c9e9e30f5c5a6 10.244.4.2:6379@16379 slave 8bac63eb45663020b4cc96b0cecdf233b54aa4be 0 1604407450000 3 connected
*/
type RedisClusterNodes []RedisClusterNode

type RedisClusterNode struct {
	ID          string
	Addr        string
	Flags       string
	IsFailing   bool
	Leader      string
	PingSend    string
	PingRecv    string
	ConfigEpoch string
	LinkState   string
}

func NewRedisInfo(rawInfo string) *RedisInfo {
	var currentInfo *map[string]string
	var currentInfoLabel string
	if rawInfo == "" {
		return nil
	}
	lines := strings.Split(rawInfo, "\n")
	currentLine := 0
	info := RedisInfo{}
	for currentLine < len(lines) {
		if lines[currentLine] != "" {
			if lines[currentLine][0] == '#' {
				currentInfoLabel = lines[currentLine][2:]
				switch currentInfoLabel {
				case "Server":
					info.Server = make(map[string]string)
					currentInfo = &info.Server
				case "Clients":
					info.Clients = make(map[string]string)
					currentInfo = &info.Clients
				case "Memory":
					info.Memory = make(map[string]string)
					currentInfo = &info.Memory
				case "Persistence":
					info.Persistence = make(map[string]string)
					currentInfo = &info.Persistence
				case "Stats":
					info.Stats = make(map[string]string)
					currentInfo = &info.Stats
				case "Replication":
					info.Replication = make(map[string]string)
					currentInfo = &info.Replication
				case "CPU":
					info.CPU = make(map[string]string)
					currentInfo = &info.CPU
				case "Modules":
					info.Modules = make(map[string]string)
					currentInfo = &info.Modules
				case "Cluster":
					info.Cluster = make(map[string]string)
					currentInfo = &info.Cluster
				case "Keyspace":
					info.Keyspace = make(map[string]string)
					currentInfo = &info.Keyspace
				}
			} else {
				lineInfo := strings.Split(lines[currentLine], ":")
				(*currentInfo)[lineInfo[0]] = lineInfo[1]
			}
		}
		currentLine++
	}
	return &info
}

func NewRedisClusterInfo(rawData string) *RedisClusterInfo {
	if rawData == "" {
		return nil
	}

	info := RedisClusterInfo{}
	lines := strings.Split(rawData, "\n")
	for _, line := range lines {
		lineInfo := strings.Split(line, ":")
		info[lineInfo[0]] = lineInfo[1]
	}
	return &info
}

// NewRedisClusterNodes is a constructor for RedisClusterNodes
func NewRedisClusterNodes(rawData string) *RedisClusterNodes {
	nodes := RedisClusterNodes{}
	nodeLines := strings.Split(rawData, "\n")
	for _, nodeLine := range nodeLines {
		nodeInfo := strings.Split(nodeLine, " ")
		if len(nodeInfo) >= 8 {

			nodes = append(nodes, RedisClusterNode{
				ID:          nodeInfo[0],
				Addr:        nodeInfo[1],
				Flags:       nodeInfo[2],
				IsFailing:   strings.Contains(nodeInfo[2], "fail"),
				Leader:      nodeInfo[3],
				PingSend:    nodeInfo[4],
				PingRecv:    nodeInfo[5],
				ConfigEpoch: nodeInfo[6],
				LinkState:   nodeInfo[7],
			})

		}
	}
	return &nodes
}
