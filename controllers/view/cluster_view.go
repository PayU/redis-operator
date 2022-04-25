package view

import (
	"github.com/PayU/redis-operator/controllers/rediscli"
	corev1 "k8s.io/api/core/v1"
)

type RedisClusterView struct {
	Pods map[string]*NodeView
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

func (v *RedisClusterView) CreateView(pods []corev1.Pod, redisCli *rediscli.RedisCLI) {
	v.Pods = make(map[string]*NodeView)
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
		v.Pods[pod.Name] = redisNode
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
