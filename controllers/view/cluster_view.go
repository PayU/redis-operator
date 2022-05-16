package view

import (
	"fmt"

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
	CreateNode    NodeState = "CreateNode"
	AddNode       NodeState = "AddNode"
	ReplicateNode NodeState = "ReplicateNode"
	SyncNode      NodeState = "SyncNode"
	ReshardNode   NodeState = "ReshardNode"
	DeleteNode    NodeState = "DeleteNode"
	NodeOK        NodeState = "NodeOK"
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
	Name        string
	NodeId      string
	Namespace   string
	Ip          string
	LeaderName  string
	IsLeader    bool
	IsReachable bool
}

type NodeStateView struct {
	Name       string
	LeaderName string
	NodeState  NodeState
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

func (v *RedisClusterView) CreateView(pods []corev1.Pod, redisCli *rediscli.RedisCLI) {
	v.Nodes = make(map[string]*NodeView)
	for _, pod := range pods {
		redisNode := &NodeView{
			Name:       pod.Name,
			NodeId:     "",
			Namespace:  pod.Namespace,
			Ip:         pod.Status.PodIP,
			LeaderName: pod.Labels["leader-name"],
			IsLeader:   pod.Labels["redis-node-role"] == "leader",
		}
		redisNode.IsReachable = isReachableNode(redisNode, redisCli)
		v.Nodes[pod.Name] = redisNode
	}
}

func isReachableNode(n *NodeView, redisCli *rediscli.RedisCLI) bool {
	var e error
	if n.NodeId, e = redisCli.MyClusterID(n.Ip); e != nil {
		return false
	}
	if clusterInfo, _, err := redisCli.ClusterInfo(n.Ip); err != nil || clusterInfo == nil || (*clusterInfo)["cluster_state"] != "ok" {
		return false
	}
	return true
}
