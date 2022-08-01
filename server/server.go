package server

import (
	"github.com/labstack/echo/v4"
)

func StartServer() {
	echoMain := echo.New()

	// Routes
	register(echoMain)

	// Start server
	go echoMain.Logger.Fatal(echoMain.Start("0.0.0.0:8080"))
}
