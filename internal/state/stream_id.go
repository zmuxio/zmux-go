package state

import (
	"fmt"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func FirstLocalStreamID(role wire.Role, bidi bool) uint64 {
	switch role {
	case wire.RoleInitiator:
		if bidi {
			return 4
		}
		return 2
	case wire.RoleResponder:
		if bidi {
			return 1
		}
		return 3
	default:
		return 0
	}
}

func FirstPeerStreamID(localRole wire.Role, bidi bool) uint64 {
	switch localRole {
	case wire.RoleInitiator:
		if bidi {
			return 1
		}
		return 3
	case wire.RoleResponder:
		if bidi {
			return 4
		}
		return 2
	default:
		return 0
	}
}

func StreamIsBidi(streamID uint64) bool {
	return streamID&0x2 == 0
}

func StreamOpener(streamID uint64) wire.Role {
	switch streamID & 0x3 {
	case 0, 2:
		return wire.RoleInitiator
	case 1, 3:
		return wire.RoleResponder
	default:
		return 0
	}
}

func StreamIsLocal(localRole wire.Role, streamID uint64) bool {
	return StreamOpener(streamID) == localRole
}

func StreamKindForLocal(localRole wire.Role, streamID uint64) (localSend, localRecv bool) {
	bidi := StreamIsBidi(streamID)
	localOpened := StreamIsLocal(localRole, streamID)
	switch {
	case bidi:
		return true, true
	case localOpened:
		return true, false
	default:
		return false, true
	}
}

func ValidateStreamIDForRole(localRole wire.Role, streamID uint64) error {
	if streamID == 0 {
		return fmt.Errorf("stream_id = 0 is session-scoped")
	}
	opener := StreamOpener(streamID)
	if opener != wire.RoleInitiator && opener != wire.RoleResponder {
		return fmt.Errorf("invalid stream_id %d", streamID)
	}
	_ = localRole
	return nil
}

func ValidateLocalOpenID(localRole wire.Role, streamID uint64, bidi bool) error {
	if err := ValidateStreamIDForRole(localRole, streamID); err != nil {
		return err
	}
	if !StreamIsLocal(localRole, streamID) {
		return fmt.Errorf("stream_id %d is not locally owned for role %s", streamID, localRole)
	}
	if StreamIsBidi(streamID) != bidi {
		want := "unidirectional"
		if bidi {
			want = "bidirectional"
		}
		return fmt.Errorf("stream_id %d is not %s", streamID, want)
	}
	return nil
}
