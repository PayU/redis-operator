package view

import (
	"github.com/PayU/redis-operator/controllers/rediscli"
	corev1 "k8s.io/api/core/v1"
)

type ClusterState int
type NodeState int

const (
	ClusterFix ClusterState = iota
	ClusterRebalance
	ClusterOK
)

const (
	CreateNode NodeState = iota
	AddNode
	ReplicateNode
	SyncNode
	ReshardNode
	DeleteNode
	NodeOK
)

func (s ClusterState) String() string {
	return [...]string{"ClusterFix", "ClusterRebalance", "ClusterOK"}[s]
}

func (n NodeState) String() string {
	return [...]string{"CreateNode", "AddNode", "ReplicateNode", "SyncNode", "ReshardNode", "DeleteNode", "NodeOK"}[n]
}

type RedisClusterView struct {
	Nodes map[string]*NodeView
}

type RedisClusterStatusView struct {
	ClusterState ClusterState
	Nodes        map[string]*NodeStatusView
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

type NodeStatusView struct {
	Name       string
	LeaderName string
	NodeState  NodeState
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
