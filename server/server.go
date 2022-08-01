package server

import (
	//"github.com/labstack/echo-contrib/prometheus"
	"github.com/labstack/echo/v4"
)

func StartServer() {
	echoMain := echo.New()
	//prom := prometheus.NewPrometheus("echo", nil)

	// Routes
	register(echoMain)
	//prom.Use(echoMain)

	// Start server
	go echoMain.Logger.Fatal(echoMain.Start("0.0.0.0:8080"))
}
