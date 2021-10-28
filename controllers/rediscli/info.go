package rediscli

import (
	"regexp"
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
type RedisClusterNodes []RedisClusterNode

type RedisClusterNode struct {
	ID          string
	Addr        string
	Flags       string
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
	info := RedisInfo{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			if line[0] == '#' {
				currentInfoLabel = strings.TrimSpace(line[2:])
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
				lineInfo := strings.Split(line, ":")
				if len(lineInfo) > 1 {
					(*currentInfo)[lineInfo[0]] = lineInfo[1]
				}

			}
		}
	}
	return &info
}

func NewRedisClusterInfo(rawData string) *RedisClusterInfo {
	if rawData == "" {
		return nil
	}

	info := RedisClusterInfo{}
	lines := strings.Split(rawData, "\r\n")
	for _, line := range lines {
		lineInfo := strings.Split(line, ":")
		if len(lineInfo) < 2 {
			return nil
		}
		info[lineInfo[0]] = lineInfo[1]
	}
	return &info
}

// NewRedisClusterNodes is a constructor for RedisClusterNodes
func NewRedisClusterNodes(rawData string) *RedisClusterNodes {
	nodes := RedisClusterNodes{}
	nodeLines := strings.Split(rawData, "\n")
	for _, nodeLine := range nodeLines {
		nodeLine := strings.TrimSpace(nodeLine)
		nodeInfo := strings.Split(nodeLine, " ")
		if strings.Contains(nodeInfo[0], ")") { // special case for CLUSTER REPLICAS output
			nodeInfo = nodeInfo[1:]
		}
		if len(nodeInfo) >= 8 {
			nodes = append(nodes, RedisClusterNode{
				ID:          nodeInfo[0],
				Addr:        nodeInfo[1],
				Flags:       nodeInfo[2],
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

// IsFailing method return true when the current redis node is in failing state
// and needs to be forgotten by the cluster
func (r *RedisClusterNode) IsFailing() bool {
	match, err := regexp.MatchString(".*fail!?\\z", r.Flags)

	if err != nil {
		return false
	}

	return match
}

// Returns the estimated completion percentage or the empty string if SYNC is
// not in progress
func (r *RedisInfo) GetSyncStatus() string {
	if r.Replication["role"] == "slave" {
		if r.Replication["master_sync_in_progress"] != "0" {
			p, found := r.Replication["master_sync_perc"]
			if found {
				return p
			}
		}
	}
	return ""
}

func (r *RedisClusterInfo) IsClusterFail() bool {
	return (*r)["cluster_state"] == "fail"
}

// GetLoadETA indicating if the load of a dump file is on-going
// If a load operation is on-going, it returns the ETA to finish.
func (r *RedisInfo) GetLoadETA() string {
	if r.Persistence["loading"] != "0" {
		eta, found := r.Persistence["loading_eta_seconds"]
		if found {
			return eta
		}
	}
	return ""
}

// Returns the IP and port for a given Redis ID or empty strings if ID not found
func (r *RedisClusterNodes) GetIPForID(id string) (string, string) {
	for _, info := range *r {
		if info.ID == id {
			ipPort := strings.Split(info.Addr, ":")
			return ipPort[0], ipPort[1]
		}
	}
	return "", ""
}

// Returns the Redis node ID for a specified IP or empty string if IP not found
// Supports the IP and IP:port format
func (r *RedisClusterNodes) GetIDForIP(ip string) string {
	ipPort := strings.Split(ip, ":")
	for _, info := range *r {
		if strings.Split(info.Addr, ":")[0] == ipPort[0] {
			return info.ID
		}
	}
	return ""
}
