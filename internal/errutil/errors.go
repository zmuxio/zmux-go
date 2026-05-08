package errutil

import "reflect"

const maxUnwrapDepth = 64

func Find[T any](err error) (T, bool) {
	return findDepth[T](err, 0)
}

func findDepth[T any](err error, depth int) (T, bool) {
	var zero T
	if err == nil || depth > maxUnwrapDepth {
		return zero, false
	}
	if target, ok := any(err).(T); ok {
		return target, true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if target, ok := findDepth[T](child, depth+1); ok {
				return target, true
			}
		}
		return zero, false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return findDepth[T](wrapped.Unwrap(), depth+1)
	}
	return zero, false
}

func Is(err, target error) bool {
	if target == nil {
		return err == nil
	}
	return isDepth(err, target, 0)
}

func isDepth(err, target error, depth int) bool {
	if err == nil || depth > maxUnwrapDepth {
		return false
	}
	if Same(err, target) {
		return true
	}
	if matcher, ok := err.(interface{ Is(error) bool }); ok && matcher.Is(target) {
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if isDepth(child, target, depth+1) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isDepth(wrapped.Unwrap(), target, depth+1)
	}
	return false
}

func Same(err, target error) bool {
	if err == nil || target == nil {
		return err == nil && target == nil
	}
	errValue := reflect.ValueOf(err)
	targetValue := reflect.ValueOf(target)
	return errValue.Type() == targetValue.Type() &&
		errValue.Comparable() &&
		errValue.Equal(targetValue)
}
