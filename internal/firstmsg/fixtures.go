// Package firstmsg provides the specification's first-message test fixtures.
package firstmsg

import "embed"

// Fixtures contains the byte vectors and scenario inputs.
//
//go:embed testdata/*.json
var Fixtures embed.FS
