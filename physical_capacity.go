package zmux

import (
	"reflect"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/core"
)

// PhysicalCarrier identifies a shareable physical connection when its
// lane identity is scoped to a smaller session on that connection. A known
// identity is comparable and stays constant throughout the handoff and I/O.
type PhysicalCarrier interface {
	PhysicalCarrier() any
}

type anonymousPhysical struct{ _ byte }

func physicalCarrierKeys(p PipeIO) core.CarrierKeys {
	read, write := unprefixed(p.R), unprefixed(p.W)
	send, receive := physicalKey(write, p.SendSource.CarrierID), physicalKey(read, p.ReceiveSource.CarrierID)
	switch {
	case p.W == nil:
		return core.CarrierKeys{receive}
	case p.R == nil:
		return core.CarrierKeys{send}
	case carrierio.SameHalf(read, write) || send == receive:
		return core.CarrierKeys{send}
	default:
		return core.CarrierKeys{send, receive}
	}
}

func physicalKey(half any, id CarrierID) any {
	if half == nil {
		return nil
	}
	if physical, ok := half.(PhysicalCarrier); ok {
		if key := physical.PhysicalCarrier(); comparableCarrierKey(key) {
			return key
		}
	}
	if id != (CarrierID{}) {
		return id
	}
	if carrier, ok := half.(CarrierConner); ok {
		if key := carrier.CarrierConn(); comparableCarrierKey(key) {
			return key
		}
	}
	return new(anonymousPhysical)
}

func comparableCarrierKey(key any) bool {
	if key == nil {
		return false
	}
	v := reflect.ValueOf(key)
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return false
	}
	// NaN and values containing it compare unequal even to themselves.
	return v.Comparable() && v.Equal(v) //nolint:gocritic // self-comparison checks map-key reflexivity.
}
