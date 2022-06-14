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
	e.POST("/forceReconcile", controllers.ForceReconcile)
	e.POST("/test", controllers.ClusterTest)
	e.POST("/testData", controllers.ClusterTestWithData)
	e.POST("/populateData", controllers.PopulateClusterWithData)
}
