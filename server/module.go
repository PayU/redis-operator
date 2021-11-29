package server

import (
	"encoding/json"
	"net/http"

	. "github.com/PayU/redis-operator/controllers"
	clusterData "github.com/PayU/redis-operator/data"
	"github.com/labstack/echo/v4"
	v1 "k8s.io/api/core/v1"
)

type RelevantRedisClusterView struct {
	State string
	Nodes []RelevantLeaderNode
}

type RelevantLeaderNode struct {
	PodIp       string
	NodeNumber  string
	Failed      bool
	Terminating bool
	Followers   []RelevantFollowerNode
}

type RelevantFollowerNode struct {
	PodIp        string
	NodeNumber   string
	LeaderNumber string
	Failed       bool
	Terminating  bool
}

func clusterInfo(c echo.Context) error {
	byteValue, err := clusterData.GetClusterView()
	if err != nil {
		return c.String(http.StatusNotFound, "Cluster info not available")
	}

	var result RedisClusterView
	json.Unmarshal([]byte(byteValue), &result)

	s := clusterData.GetRedisClusterState()
	relevantRedisClusterView := RelevantRedisClusterView{
		State: s,
		Nodes: make([]RelevantLeaderNode, len(result)),
	}

	for i, leaderNode := range result {
		ip := getIP(leaderNode.Pod)

		relevantRedisClusterView.Nodes[i] = RelevantLeaderNode{
			Followers:   make([]RelevantFollowerNode, len(leaderNode.Followers)),
			PodIp:       ip,
			NodeNumber:  leaderNode.NodeNumber,
			Failed:      leaderNode.Failed,
			Terminating: leaderNode.Terminating,
		}
		for j, follower := range leaderNode.Followers {
			followerIp := getIP(follower.Pod)
			relevantRedisClusterView.Nodes[i].Followers[j] = RelevantFollowerNode{
				PodIp:        followerIp,
				NodeNumber:   follower.NodeNumber,
				LeaderNumber: follower.LeaderNumber,
				Failed:       follower.Failed,
				Terminating:  leaderNode.Terminating,
			}
		}
	}

	return c.JSON(http.StatusOK, relevantRedisClusterView)
}

func getIP(pod *v1.Pod) string {
	if pod == nil {
		return ""
	} else {
		return pod.Status.PodIP
	}
}

func clusterState(c echo.Context) error {
	s := clusterData.GetRedisClusterState()
	return c.String(http.StatusOK, s)
}
