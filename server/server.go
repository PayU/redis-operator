package server

import (
	"github.com/labstack/echo/v4"
)

func StartServer() {
	e := echo.New()

	// Routes
	Register(e)

	// Start server
	go e.Logger.Fatal(e.Start("0.0.0.0:8080"))
}
