package server

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func Register(e *echo.Echo) {
	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})
	e.GET("/status", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})
	e.GET("/Info", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})
}
