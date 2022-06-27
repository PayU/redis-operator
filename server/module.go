package server

import (
	"encoding/json"
	"net/http"

	"github.com/PayU/redis-operator/controllers/view"
	clusterData "github.com/PayU/redis-operator/data"
	"github.com/labstack/echo/v4"
)

type ResponseRedisClusterView struct {
	State       string
	ClusterView view.RedisClusterView
}

func clusterInfo(c echo.Context) error {
	byteValue, err := clusterData.GetClusterView()
	if err != nil {
		return c.String(http.StatusNotFound, "Cluster info not available")
	}

	var result view.RedisClusterView
	json.Unmarshal([]byte(byteValue), &result)

	s := clusterData.GetRedisClusterState()
	ResponseRedisClusterView := ResponseRedisClusterView{
		State:       s,
		ClusterView: result,
	}

	return c.JSON(http.StatusOK, ResponseRedisClusterView)
}

func clusterState(c echo.Context) error {
	s := clusterData.GetRedisClusterState()
	return c.String(http.StatusOK, s)
}
