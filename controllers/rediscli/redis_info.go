package rediscli

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/go-logr/logr"
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

// LeaderReplicas is a result of CLUSTER REPLICA <node_id> command: https://redis.io/commands/cluster-replicas
type LeaderReplicas struct {
	Replicas []LeaderReplica
	Count    int32
}

type LeaderReplica struct {
	ID   string
	Addr string
}

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

// NewLeaderReplicas is a constructor for CLUSTER REPLICAS node-id command raw data
func NewLeaderReplicas(rawData string, log logr.Logger) *LeaderReplicas {
	replicas := make([]LeaderReplica, 0)
	replicasLines := strings.Split(rawData, "\n")

	for _, replicaLine := range replicasLines {
		replicaInfo := strings.Split(replicaLine, " ")

		log.V(9).Info(fmt.Sprintf("leader replica info: %v", replicaInfo))

		if len(replicaInfo) < 2 {
			continue
		}

		replicaID := replicaInfo[0][1:]
		replicaAddr := replicaInfo[2]

		replicas = append(replicas, LeaderReplica{
			ID:   replicaID,
			Addr: replicaAddr,
		})
	}

	return &LeaderReplicas{
		Replicas: replicas,
		Count:    int32(len(replicas)),
	}
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
