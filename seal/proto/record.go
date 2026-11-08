package proto

import "github.com/zmuxio/zmux-go/internal/join"

// Sealer protects the records of one direction, without framing. It is not
// safe for concurrent use.
type Sealer = join.Sealer

// Opener verifies the records of one direction. Any failure is permanent. It
// is not safe for concurrent use.
type Opener = join.Opener
