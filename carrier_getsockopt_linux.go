//go:build linux && !386

package zmux

import (
	"syscall"
	"unsafe"
)

// getsockopt reads a socket option of any size into p.
func getsockopt(fd, level, opt uintptr, p unsafe.Pointer, l *uint32) syscall.Errno {
	_, _, e := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, level, opt,
		uintptr(p), uintptr(unsafe.Pointer(l)), 0)
	return e
}
