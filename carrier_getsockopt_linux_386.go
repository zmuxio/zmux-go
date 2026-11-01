package zmux

import (
	"syscall"
	"unsafe"
)

// sysGetsockopt is the socketcall(2) number of getsockopt.
const sysGetsockopt = 15

// getsockopt reads a socket option of any size into p. On 386 the socket
// calls go through socketcall(2), which takes its arguments as an array.
func getsockopt(fd, level, opt uintptr, p unsafe.Pointer, l *uint32) syscall.Errno {
	args := [5]uintptr{fd, level, opt, uintptr(p), uintptr(unsafe.Pointer(l))}
	_, _, e := syscall.Syscall(syscall.SYS_SOCKETCALL, sysGetsockopt, uintptr(unsafe.Pointer(&args[0])), 0)
	return e
}
