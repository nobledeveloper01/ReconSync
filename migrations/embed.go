// Package migrations embeds the schema so a binary always carries the schema it
// expects. A container running one against another is the failure this avoids.
package migrations

import "embed"

// FS holds every .sql file in this directory.
//
//go:embed *.sql
var FS embed.FS
