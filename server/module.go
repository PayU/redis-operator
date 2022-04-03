package server

import (
	"encoding/json"
	"net/http"

	"github.com/PayU/redis-operator/controllers/view"
	clusterData "github.com/PayU/redis-operator/data"
	"github.com/labstack/echo/v4"
)

type ResponseRedisClusterView struct {
	State string
	Nodes view.PrintableClusterView
}

type ResponseLeaderNode struct {
	PodIp       string
	NodeNumber  string
	Failed      bool
	Terminating bool
	Followers   []ResponseFollowerNode
}

type ResponseFollowerNode struct {
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

	var v view.PrintableClusterView
	json.Unmarshal([]byte(byteValue), &v)

	s := clusterData.GetRedisClusterState()
	ResponseRedisClusterView := ResponseRedisClusterView{
		State: s,
		Nodes: v,
	}

	return c.JSON(http.StatusOK, ResponseRedisClusterView)
}

func clusterState(c echo.Context) error {
	s := clusterData.GetRedisClusterState()
	return c.String(http.StatusOK, s)
}
