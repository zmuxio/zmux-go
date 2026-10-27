//go:build windows

package zmux

import (
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// Go's monotonic clock on Windows advances with the timer interrupt
// (0.5-15.6 ms), which reads loopback round trips as 0; the core takes its
// timestamps from QueryPerformanceCounter there.
var (
	qpcOnce  sync.Once
	qpcFreq  int64
	qpcStart int64
	qpcBase  time.Time
	qpcProc  *syscall.Proc
)

func qpcInit() {
	dll, err := syscall.LoadDLL("kernel32.dll")
	if err != nil {
		return
	}
	f, err1 := dll.FindProc("QueryPerformanceFrequency")
	c, err2 := dll.FindProc("QueryPerformanceCounter")
	if err1 != nil || err2 != nil {
		return
	}
	var freq, now int64
	if r, _, _ := f.Call(uintptr(unsafe.Pointer(&freq))); r == 0 || freq == 0 {
		return
	}
	// QueryPerformanceCounter cannot fail on Windows XP and later.
	_, _, _ = c.Call(uintptr(unsafe.Pointer(&now)))
	qpcFreq, qpcStart, qpcBase, qpcProc = freq, now, time.Now(), c
}

func monoSince(base time.Time) int64 {
	qpcOnce.Do(qpcInit)
	if qpcProc == nil {
		return int64(time.Since(base))
	}
	var now int64
	_, _, _ = qpcProc.Call(uintptr(unsafe.Pointer(&now))) // cannot fail
	ticks := now - qpcStart
	ns := ticks/qpcFreq*1e9 + ticks%qpcFreq*1e9/qpcFreq
	return int64(qpcBase.Sub(base)) + ns
}
