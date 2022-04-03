package view

import (
	corev1 "k8s.io/api/core/v1"
)

type ClusterView struct {
	ViewByPodName    map[string]*ClusterNodeView
	RedisIdToPodName map[string]string
}

type PrintableClusterView []*PrintableClusterNodeView

type ClusterNodeView struct {
	Name            string
	Namespace       string
	Status          string
	ID              string
	IP              string
	Port            string
	Leader          string
	FollowersByName []string
	IsLeader        bool
	IsReachable     bool
	Pod             corev1.Pod
}

type PrintableClusterNodeView struct {
	Name            string
	Namespace       string
	Status          string
	ID              string
	IP              string
	Port            string
	Leader          string
	FollowersByName []string
	IsLeader        bool
	IsReachable     bool
}

func (v *ClusterView) HealthyNodesIPs() []string {
	var ips []string
	for _, node := range v.ViewByPodName {
		if node.Status == "Running" && node.IsReachable {
			ips = append(ips, node.IP)
		}
	}
	return ips
}

func (n *ClusterNodeView) IsIdentical(other *ClusterNodeView) bool {
	return n.Name == other.Name &&
		n.Namespace == other.Namespace &&
		n.Status == other.Status &&
		n.ID == other.ID &&
		n.IP == other.IP &&
		n.Port == other.Port &&
		n.Leader == other.Leader &&
		n.IsLeader == other.IsLeader &&
		n.IsReachable == other.IsReachable
}

func (v *ClusterView) ToPrintableFormat() PrintableClusterView {
	nodesView := make([]*PrintableClusterNodeView, 0)
	for _, node := range v.ViewByPodName {
		nodesView = append(nodesView, node.ToPrintableFormat())
	}
	return nodesView
}

func (n *ClusterNodeView) ToPrintableFormat() *PrintableClusterNodeView {
	return &PrintableClusterNodeView{
		Name:            n.Name,
		Namespace:       n.Namespace,
		Status:          n.Status,
		ID:              n.ID,
		IP:              n.IP,
		Port:            n.Port,
		Leader:          n.Leader,
		FollowersByName: n.FollowersByName,
		IsLeader:        n.IsLeader,
		IsReachable:     n.IsReachable,
	}
}
