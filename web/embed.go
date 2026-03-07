package web

import "embed"

//go:embed all:static
var StaticFS embed.FS
