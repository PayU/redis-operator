package server

import (
	"github.com/PayU/redis-operator/controllers"
	"github.com/labstack/echo/v4"
)

func Register(e *echo.Echo) {
	e.GET("/state", clusterState)
	e.GET("/info", clusterInfo)
	e.GET("/reset", controllers.DoResetCluster)
	e.GET("/addLeaders", controllers.AddNewLeaders)
	e.GET("/delLeaders", controllers.DeleteNewLeaders)
	e.GET("/clusterRebalance", controllers.ClusterRebalance)
	e.GET("/clusterFix", controllers.ClusterFix)
	e.GET("/stateView", controllers.PrintClusterStateView)
}
