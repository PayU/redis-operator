package server

import (
	"github.com/labstack/echo/v4"
)

func StartServer() {
	echo := echo.New()

	// Routes
	Register(echo)

	// Start server
	go echo.Logger.Fatal(echo.Start("0.0.0.0:8080"))
}
