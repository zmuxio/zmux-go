package wire

import "sync"

const maxRetainedFrameReadBufferOverhead = 17

type FrameReadBufferHandle struct {
	bucket int
	buf    []byte
}

type frameReadBufferBucket struct {
	size int
	pool sync.Pool
}

const minFrameReadBufferBucketSize = 64

var frameReadBufferBuckets = newFrameReadBufferBuckets()

func newFrameReadBufferBuckets() []frameReadBufferBucket {
	maxSize := int(DefaultSettings().MaxFramePayload) + maxRetainedFrameReadBufferOverhead
	buckets := make([]frameReadBufferBucket, 0, 16)
	for size := minFrameReadBufferBucketSize; size <= maxSize; size <<= 1 {
		bucketIndex := len(buckets)
		bucketSize := size
		buckets = append(buckets, frameReadBufferBucket{
			size: bucketSize,
			pool: sync.Pool{
				New: func() any {
					return &FrameReadBufferHandle{
						bucket: bucketIndex,
						buf:    make([]byte, 0, bucketSize),
					}
				},
			},
		})
		if size > maxSize/2 {
			break
		}
	}
	return buckets
}

func frameReadBufferBucketIndex(n int) int {
	if n <= 0 {
		return -1
	}
	for i := range frameReadBufferBuckets {
		if n <= frameReadBufferBuckets[i].size {
			return i
		}
	}
	return -1
}

func acquireFrameReadBuffer(n int) ([]byte, *FrameReadBufferHandle) {
	if n <= 0 {
		return nil, nil
	}
	if idx := frameReadBufferBucketIndex(n); idx >= 0 {
		handle := frameReadBufferBuckets[idx].pool.Get().(*FrameReadBufferHandle)
		return handle.buf[:n], handle
	}
	return make([]byte, n), nil
}

func ReleaseReadFrameBuffer(buf []byte, handle *FrameReadBufferHandle) {
	if handle == nil {
		return
	}
	if handle.bucket < 0 || handle.bucket >= len(frameReadBufferBuckets) {
		return
	}
	bucket := &frameReadBufferBuckets[handle.bucket]
	handle.buf = retainedFrameReadBuffer(buf, handle, bucket.size)
	bucket.pool.Put(handle)
}

func retainedFrameReadBuffer(buf []byte, handle *FrameReadBufferHandle, bucketSize int) []byte {
	if handle == nil || bucketSize <= 0 {
		return nil
	}
	if cap(handle.buf) != bucketSize {
		return make([]byte, 0, bucketSize)
	}
	if cap(buf) == bucketSize && sameBufferBackingStart(buf, handle.buf) {
		return buf[:0]
	}
	return handle.buf[:0]
}

func sameBufferBackingStart(a, b []byte) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	return &a[:1][0] == &b[:1][0]
}
