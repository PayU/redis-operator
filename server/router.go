package server

import (
	"github.com/PayU/redis-operator/controllers"
	"github.com/labstack/echo/v4"
)

func register(e *echo.Echo) {
	e.GET("/state", controllers.ClusterState)
	e.GET("/info", controllers.ClusterInfo)
	e.POST("/rebalance", controllers.ClusterRebalance)
	e.POST("/fix", controllers.ClusterFix)
	e.POST("/forgetLostNodes", controllers.ForgetLostNodes)
	e.POST("/forceReconcile", controllers.ForceReconcile)
	e.POST("/upgrade", controllers.UpgradeCluster)
	e.POST("/test", controllers.ClusterTest)
	e.POST("/reset", controllers.DoResetCluster)
	e.POST("/testData", controllers.ClusterTestWithData)
	e.POST("/populateMockData", controllers.PopulateClusterWithMockData)
	e.POST("/flushAllData", controllers.FlushClusterData)
}
