package server

import (
	"encoding/json"
	"net/http"

	. "github.com/PayU/redis-operator/controllers"
	clusterData "github.com/PayU/redis-operator/data"
	"github.com/labstack/echo/v4"
	v1 "k8s.io/api/core/v1"
)

type ResponseRedisClusterView struct {
	State string
	Nodes []ResponseLeaderNode
}

type ResponseLeaderNode struct {
	PodIp       string
	NodeName    string
	Failed      bool
	Terminating bool
	Followers   []ResponseFollowerNode
}

type ResponseFollowerNode struct {
	PodIp       string
	NodeName    string
	LeaderName  string
	Failed      bool
	Terminating bool
}

func clusterInfo(c echo.Context) error {
	byteValue, err := clusterData.GetClusterView()
	if err != nil {
		return c.String(http.StatusNotFound, "Cluster info not available")
	}

	var result RedisClusterView
	json.Unmarshal([]byte(byteValue), &result)

	s := clusterData.GetRedisClusterState()
	ResponseRedisClusterView := ResponseRedisClusterView{
		State: s,
		Nodes: make([]ResponseLeaderNode, len(result)),
	}

	for i, leaderNode := range result {
		ip := getIP(leaderNode.Pod)

		ResponseRedisClusterView.Nodes[i] = ResponseLeaderNode{
			Followers:   make([]ResponseFollowerNode, len(leaderNode.Followers)),
			PodIp:       ip,
			NodeName:    leaderNode.NodeName,
			Failed:      leaderNode.Failed,
			Terminating: leaderNode.Terminating,
		}
		for j, follower := range leaderNode.Followers {
			followerIp := getIP(follower.Pod)
			ResponseRedisClusterView.Nodes[i].Followers[j] = ResponseFollowerNode{
				PodIp:       followerIp,
				NodeName:    follower.NodeName,
				LeaderName:  follower.LeaderName,
				Failed:      follower.Failed,
				Terminating: leaderNode.Terminating,
			}
		}
	}

	return c.JSON(http.StatusOK, ResponseRedisClusterView)
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
