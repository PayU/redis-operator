package view

import (
	"errors"
	"fmt"
	"sync"

	"github.com/PayU/redis-operator/controllers/rediscli"
	corev1 "k8s.io/api/core/v1"
)

type ClusterState string
type NodeState string

const (
	ClusterCreate    ClusterState = "ClusterCreate"
	ClusterFix       ClusterState = "ClusterFix"
	ClusterRebalance ClusterState = "ClusterRebalance"
	ClusterOK        ClusterState = "ClusterOK"
)

const (
	CreateNode           NodeState = "CreateNode"
	AddNode              NodeState = "AddNode"
	ReplicateNode        NodeState = "ReplicateNode"
	SyncNode             NodeState = "SyncNode"
	FailoverNode         NodeState = "FailoverNode"
	ReshardNode          NodeState = "ReshardNode"
	ReshardNodeKeepInMap NodeState = "ReshardNodeKeepInMap"
	NewEmptyNode         NodeState = "NewEmptyNode"
	DeleteNode           NodeState = "DeleteNode"
	DeleteNodeKeepInMap  NodeState = "DeleteNodeKeepInMap"
	NodeOK               NodeState = "NodeOK"
)

type RedisClusterView struct {
	Nodes map[string]*NodeView
}

type RedisClusterStateView struct {
	Name         string
	ClusterState ClusterState
	Nodes        map[string]*NodeStateView
}

type NodeView struct {
	Name       string
	Id         string
	Namespace  string
	Ip         string
	LeaderName string
	IsLeader   bool
	Pod        corev1.Pod
}

type NodeStateView struct {
	Name       string
	LeaderName string
	NodeState  NodeState
}

type MissingNodeView struct {
	Name              string
	LeaderName        string
	CurrentMasterName string
	CurrentMasterId   string
	CurrentMasterIp   string
}

func (sv *RedisClusterStateView) CreateStateView(leaderCount int, followersPerLeaderCount int) {
	sv.Name = "cluster-state-map"
	sv.ClusterState = ClusterCreate
	sv.Nodes = make(map[string]*NodeStateView)
	for l := 0; l < leaderCount; l++ {
		name := "redis-node-" + fmt.Sprint(l)
		sv.Nodes[name] = &NodeStateView{
			Name:       name,
			LeaderName: name,
			NodeState:  CreateNode,
		}
	}
	for _, leader := range sv.Nodes {
		if leader.Name == leader.LeaderName {
			for f := 1; f <= followersPerLeaderCount; f++ {
				name := leader.Name + "-" + fmt.Sprint(f)
				sv.Nodes[name] = &NodeStateView{
					Name:       name,
					LeaderName: leader.Name,
					NodeState:  CreateNode,
				}
			}
		}
	}
}

func (sv *RedisClusterStateView) SetNodeState(name string, leaderName string, nodeState NodeState) {
	n, exists := sv.Nodes[name]
	if exists {
		n.NodeState = nodeState
	} else {
		sv.Nodes[name] = &NodeStateView{
			Name:       name,
			LeaderName: leaderName,
			NodeState:  nodeState,
		}
	}
}

func (sv *RedisClusterStateView) LockResourceAndSetNodeState(name string, leaderName string, nodeState NodeState, mutex *sync.Mutex) {
	mutex.Lock()
	n, exists := sv.Nodes[name]
	if exists {
		n.NodeState = nodeState
	} else {
		sv.Nodes[name] = &NodeStateView{
			Name:       name,
			LeaderName: leaderName,
			NodeState:  nodeState,
		}
	}
	mutex.Unlock()
}

func (sv *RedisClusterStateView) LockResourceAndRemoveFromMap(name string, mutex *sync.Mutex) {
	mutex.Lock()
	delete(sv.Nodes, name)
	mutex.Unlock()
}

func (v *RedisClusterView) CreateView(pods []corev1.Pod, redisCli *rediscli.RedisCLI) error {
	v.Nodes = make(map[string]*NodeView)
	for _, pod := range pods {
		redisNode := &NodeView{
			Name:       pod.Name,
			Id:         "",
			Namespace:  pod.Namespace,
			Ip:         pod.Status.PodIP,
			LeaderName: pod.Labels["leader-name"],
			IsLeader:   pod.Labels["redis-node-role"] == "leader",
			Pod:        pod,
		}
		if !isReachableNode(redisNode, redisCli) {
			return errors.New("Non reachable node found")
		}
		v.Nodes[pod.Name] = redisNode
	}
	return nil
}

func isReachableNode(n *NodeView, redisCli *rediscli.RedisCLI) bool {
	var e error
	if n.Id, e = redisCli.MyClusterID(n.Ip); e != nil {
		return false
	}
	nodes, _, err := redisCli.ClusterNodes(n.Ip)
	if err != nil || nodes == nil || *nodes == nil {
		return false
	} else if len(*nodes) == 1 {
		return true
	}
	if clusterInfo, _, err := redisCli.ClusterInfo(n.Ip); err != nil || clusterInfo == nil || (*clusterInfo)["cluster_state"] != "ok" {
		return false
	}
	return true
}
