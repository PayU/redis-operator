package server

import (
	"github.com/labstack/echo/v4"
)

func StartServer() {
	echo := echo.New()

	// Routes
	register(echo)

	// Start server
	go echo.Logger.Fatal(echo.Start("0.0.0.0:8080"))
}
