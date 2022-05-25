package server

import (
	"github.com/PayU/redis-operator/controllers"
	"github.com/labstack/echo/v4"
)

func Register(e *echo.Echo) {
	e.GET("/state", clusterState)
	e.GET("/info", clusterInfo)
	e.POST("/reset", controllers.DoResetCluster)
	e.POST("/rebalance", controllers.ClusterRebalance)
	e.POST("/fix", controllers.ClusterFix)
	e.POST("/reconcile", controllers.DoReconcile)
}
