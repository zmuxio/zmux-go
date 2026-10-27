package zmux

import (
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
)

// readBufSize holds one maximal record plus the start of the next one
// (at least max_record_size + 4).
const readBufSize = 128 << 10

// idleBufSize is the small buffer a reader blocks on while its pipe is
// quiet, so that an idle pipe holds no pooled buffer.
const idleBufSize = 256

// readPool holds free read buffers for reuse by every session; the garbage
// collector trims it, so idle processes keep none.
var readPool sync.Pool

func getReadBuf() *core.Buf {
	b, _ := readPool.Get().(*core.Buf)
	if b == nil {
		b = &core.Buf{B: make([]byte, readBufSize), Free: putReadBuf}
	}
	b.Ref()
	return b
}

func putReadBuf(b *core.Buf) { readPool.Put(b) }
