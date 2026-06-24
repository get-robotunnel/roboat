// Package db embeds the tunnel SQL migrations so tunnel-svc can apply them on
// startup without shipping a separate migrations directory.
package db

import "embed"

//go:embed migrations/*.sql
var Migrations embed.FS
