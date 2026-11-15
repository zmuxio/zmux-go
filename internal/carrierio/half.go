package carrierio

import "reflect"

// SameHalf reports whether two halves of a carrier are one value. The halves come
// from the host and may be of a type, or hold a value, that cannot be
// compared; those count as two.
func SameHalf(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return reflect.ValueOf(a).Comparable() && reflect.ValueOf(b).Comparable() && a == b
}
