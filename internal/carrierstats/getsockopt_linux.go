//go:build linux && !386

package carrierstats

import (
	"syscall"
	"unsafe"
)

// getsockopt reads a socket option of any size into p.
func getsockopt(fd, level, opt uintptr, p unsafe.Pointer, l *uint32) syscall.Errno {
	//nolint:gosec // The option buffer and its length are passed to getsockopt.
	_, _, e := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, level, opt,
		uintptr(p), uintptr(unsafe.Pointer(l)), 0)
	return e
}
