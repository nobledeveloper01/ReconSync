// Package embed carries the built dashboard into the binary.
//
// Separate from the web sources so the Go build never depends on node being
// installed: dist/ is committed, and `make web` regenerates it. A contributor
// who never touches the dashboard builds and tests the server exactly as
// before.
package embed

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

// FS returns the dashboard rooted at its index, or nil when no build is
// present — in which case the server simply serves no dashboard rather than
// failing to start.
func FS() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return nil
	}
	return sub
}
