package view

import (
	"github.com/PayU/redis-operator/controllers/rediscli"
	corev1 "k8s.io/api/core/v1"
)

type RedisClusterView struct {
	PodsViewByName  map[string]*PodView
	NodeIdToPodName map[string]string
	Terminating     []corev1.Pod
	NonReachable    []*PodView
}

type PrintableRedisClusterView map[string]PrintablePodView

type PodView struct {
	Name            string
	NodeId          string
	Namespace       string
	Ip              string
	LeaderName      string
	IsLeader        bool
	FollowersByName []string
	Pod             corev1.Pod
}

type PrintablePodView struct {
	Name            string
	NodeId          string
	Namespace       string
	Ip              string
	LeaderName      string
	IsLeader        bool
	FollowersByName []string
}

func (v *RedisClusterView) CreateView(pods []corev1.Pod, redisCli *rediscli.RedisCLI) {
	v.PodsViewByName = make(map[string]*PodView)
	v.NodeIdToPodName = make(map[string]string)
	v.Terminating = make([]corev1.Pod, 0)
	v.NonReachable = make([]*PodView, 0)
	v.analyzePods(pods, redisCli)
	v.linkLedersToFollowers()
}

func (v *RedisClusterView) analyzePods(pods []corev1.Pod, redisCli *rediscli.RedisCLI) {
	for _, pod := range pods {
		if pod.Status.Phase == "Terminating" {
			v.Terminating = append(v.Terminating, pod)
			continue
		}
		node := &PodView{
			Name:            pod.Name,
			NodeId:          "",
			Namespace:       pod.Namespace,
			Ip:              pod.Status.PodIP,
			LeaderName:      pod.Labels["leader-name"],
			IsLeader:        pod.Labels["redis-node-role"] == "leader",
			FollowersByName: make([]string, 0),
			Pod:             pod,
		}
		if node.isReachable(redisCli) {
			v.PodsViewByName[node.Name] = node
			v.NodeIdToPodName[node.NodeId] = node.Name
		} else {
			v.NonReachable = append(v.NonReachable, node)
		}
	}
}

func (v *RedisClusterView) linkLedersToFollowers() {
	for _, node := range v.PodsViewByName {
		if !node.IsLeader {
			if _, exists := v.PodsViewByName[node.LeaderName]; exists {
				v.PodsViewByName[node.LeaderName].FollowersByName = append(v.PodsViewByName[node.LeaderName].FollowersByName, node.Name)
			}
		}
	}
}

func (p *PodView) isReachable(redisCli *rediscli.RedisCLI) bool {
	var e error
	if p.NodeId, e = redisCli.MyClusterID(p.Ip); e != nil {
		return false
	}
	if clusterInfo, _, err := redisCli.ClusterInfo(p.Ip); err != nil || clusterInfo == nil || (*clusterInfo)["cluster_state"] != "ok" {
		return false
	}
	return true
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
			FollowersByName: podView.FollowersByName,
		}
	}
	return printableView
}
