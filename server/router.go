package server

import (
	"github.com/PayU/redis-operator/controllers"
	"github.com/labstack/echo/v4"
)

func Register(e *echo.Echo) {
	e.GET("/state", clusterState)
	e.GET("/info", clusterInfo)
	e.GET("/reset", controllers.DoResetCluster)
	e.GET("/updateview", controllers.UpdateExpectedView)
	e.GET("/getview", controllers.GetExpectedView)
	e.GET("/addLeaders", controllers.AddNewLeaders)
	e.GET("/delLeaders", controllers.DeleteNewLeaders)
}
