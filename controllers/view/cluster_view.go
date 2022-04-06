package view

import (
	corev1 "k8s.io/api/core/v1"
)

type RedisClusterView struct {
	PodsViewByName  map[string]*PodView
	NodeIdToPodName map[string]string
}

type PrintableRedisClusterView struct {
	PrintablePodsView map[string]*PrintablePodView
}

type PodView struct {
	Name              string
	NodeId            string
	Namespace         string
	Ip                string
	LeaderName        string
	IsLeader          bool
	IsReachable       bool
	ClusterNodesTable map[string]*TableNodeView
	FollowersByName   []string
	Pod               corev1.Pod
}

type PrintablePodView struct {
	Name              string
	NodeId            string
	Namespace         string
	Ip                string
	LeaderName        string
	IsLeader          bool
	IsReachable       bool
	ClusterNodesTable map[string]*TableNodeView
	FollowersByName   []string
}

type TableNodeView struct {
	Id          string
	LeaderId    string
	IsLeader    bool
	IsReachable bool
}

func (v *RedisClusterView) ToPrintableForm() *PrintableRedisClusterView {
	printableView := &PrintableRedisClusterView{}
	for _, podView := range v.PodsViewByName {
		printableView.PrintablePodsView[podView.Name] = &PrintablePodView{
			Name:              podView.Name,
			NodeId:            podView.NodeId,
			Namespace:         podView.Namespace,
			Ip:                podView.Ip,
			LeaderName:        podView.LeaderName,
			IsLeader:          podView.IsLeader,
			IsReachable:       podView.IsReachable,
			ClusterNodesTable: podView.ClusterNodesTable,
		}
	}
	return printableView
}
