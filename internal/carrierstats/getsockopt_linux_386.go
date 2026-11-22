package carrierstats

import (
	"syscall"
	"unsafe"
)

// sysGetsockopt is the socketcall(2) number of getsockopt.
const sysGetsockopt = 15

// getsockopt reads a socket option of any size into p. On 386 the socket
// calls go through socketcall(2), which takes its arguments as an array.
func getsockopt(fd, level, opt uintptr, p unsafe.Pointer, l *uint32) syscall.Errno {
	//nolint:gosec // socketcall takes a pointer to the argument array.
	args := [5]uintptr{fd, level, opt, uintptr(p), uintptr(unsafe.Pointer(l))}
	//nolint:gosec // socketcall takes a pointer to the argument array.
	_, _, e := syscall.Syscall(syscall.SYS_SOCKETCALL, sysGetsockopt, uintptr(unsafe.Pointer(&args[0])), 0)
	return e
}
