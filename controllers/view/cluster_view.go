package view

import (
	"strings"

	"github.com/PayU/redis-operator/controllers/rediscli"
	corev1 "k8s.io/api/core/v1"
)

type RedisClusterView struct {
	PodsViewByName  map[string]*PodView
	NodeIdToPodName map[string]string
}

type PrintableRedisClusterView map[string]PrintablePodView

type PodView struct {
	Name              string
	NodeId            string
	Namespace         string
	Ip                string
	LeaderName        string
	IsLeader          bool
	IsReachable       bool
	IsTerminating     bool
	Exists            bool
	ClusterNodesTable map[string]TableNodeView
	FollowersByName   []string
	Pod               corev1.Pod
}

type PrintablePodView struct {
	Name            string
	NodeId          string
	Namespace       string
	Ip              string
	LeaderName      string
	IsLeader        bool
	IsReachable     bool
	Exists          bool
	FollowersByName []string
}

type TableNodeView struct {
	Id          string
	LeaderId    string
	IsLeader    bool
	IsReachable bool
}

func (v *RedisClusterView) CreateView(pods []corev1.Pod, redisCli *rediscli.RedisCLI) {
	v.analyzePods(pods, redisCli)
	v.linkLedersToFollowers()
}

func (v *RedisClusterView) analyzePods(pods []corev1.Pod, redisCli *rediscli.RedisCLI) {
	for _, pod := range pods {
		node := &PodView{
			Name:              pod.Name,
			NodeId:            "",
			Namespace:         pod.Namespace,
			Ip:                pod.Status.PodIP,
			LeaderName:        pod.Labels["leader-name"],
			IsLeader:          pod.Labels["redis-node-role"] == "leader",
			IsReachable:       false,
			IsTerminating:     false,
			Exists:            true,
			ClusterNodesTable: make(map[string]TableNodeView),
			FollowersByName:   make([]string, 0),
			Pod:               pod,
		}
		v.PodsViewByName[node.Name] = node
		node.validateReachableAndGetId(redisCli)
		v.NodeIdToPodName[node.NodeId] = node.Name
	}
}

func (v *RedisClusterView) linkLedersToFollowers() {
	for _, node := range v.PodsViewByName {
		if node.IsReachable {
			if !node.IsLeader {
				if _, exists := v.PodsViewByName[node.LeaderName]; exists {
					v.PodsViewByName[node.LeaderName].FollowersByName = append(v.PodsViewByName[node.LeaderName].FollowersByName, node.Name)
				} else {
					v.PodsViewByName[node.LeaderName] = &PodView{
						Name:              node.LeaderName,
						NodeId:            "",
						Namespace:         node.Namespace,
						Ip:                "",
						LeaderName:        node.LeaderName,
						IsLeader:          true,
						IsReachable:       false,
						IsTerminating:     false,
						Exists:            false,
						ClusterNodesTable: make(map[string]TableNodeView),
						FollowersByName:   []string{node.Name},
						Pod:               corev1.Pod{},
					}
				}
			}
			for _, tableNode := range node.ClusterNodesTable {
				if _, exists := v.NodeIdToPodName[tableNode.Id]; exists {
					tableNode.IsReachable = true
				}
			}
		}
	}
}

func (p *PodView) validateReachableAndGetId(redisCli *rediscli.RedisCLI) {
	var e error
	if p.NodeId, e = redisCli.MyClusterID(p.Ip); e != nil {
		return
	}
	if p.Pod.ObjectMeta.DeletionTimestamp != nil {
		p.IsTerminating = true
		return
	}
	if clusterInfo, _, err := redisCli.ClusterInfo(p.Ip); err != nil || clusterInfo == nil || (*clusterInfo)["cluster_state"] != "ok" {
		return
	}
	var clusterNodes *rediscli.RedisClusterNodes
	if clusterNodes, _, e = redisCli.ClusterNodes(p.Ip); e != nil || clusterNodes == nil {
		return
	}
	p.IsReachable = true
	p.fillClusterTable(clusterNodes)
}

func (p *PodView) fillClusterTable(clusterNodes *rediscli.RedisClusterNodes) {
	for _, clusterNode := range *clusterNodes {
		p.ClusterNodesTable[clusterNode.ID] = TableNodeView{
			Id:          clusterNode.ID,
			LeaderId:    clusterNode.Leader,
			IsLeader:    strings.Contains(clusterNode.Flags, "master"),
			IsReachable: !strings.Contains(clusterNode.Flags, "fail"),
		}
	}
}

func (v *RedisClusterView) ToPrintableForm() PrintableRedisClusterView {
	printableView := make(map[string]PrintablePodView)
	for _, podView := range v.PodsViewByName {
		printableView[podView.Name] = PrintablePodView{
			Name:            podView.Name,
			NodeId:          podView.NodeId,
			Namespace:       podView.Namespace,
			Ip:              podView.Ip,
			LeaderName:      podView.LeaderName,
			IsLeader:        podView.IsLeader,
			IsReachable:     podView.IsReachable,
			Exists:          podView.Exists,
			FollowersByName: podView.FollowersByName,
		}
	}
	return printableView
}
