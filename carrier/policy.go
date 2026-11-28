package carrier

import (
	"fmt"
	"time"

	"github.com/zmuxio/zmux-go"
)

// ConnectionPolicy decides the physical connections and leases a pool keeps.
type ConnectionPolicy interface {
	Decide(*ConnectionView, *ConnectionDecision) error
}

// ConnectionView is a snapshot of a physical connection pool.
type ConnectionView struct {
	Members       []ConnectionMember
	Failures      int
	SinceLastDial time.Duration
	Dialing       int
}

// ConnectionMember describes one connection and its outstanding leases.
type ConnectionMember struct {
	FateGroup                uint64
	Age, Idle                time.Duration
	Pipes                    int
	BytesSent, BytesReceived uint64
	Usable                   bool
}

// ConnectionDecision sets desired physical pool capacity and retirements.
type ConnectionDecision struct {
	RetireConnections  []uint64
	Liveness           zmux.PipeLiveness
	Count              int
	DialAfter, Recheck time.Duration
}

// Validate checks the physical pool decision.
func (d *ConnectionDecision) Validate() error {
	if d.Count < 0 || d.DialAfter < 0 || d.Recheck < 0 {
		return fmt.Errorf("%w: invalid physical pool decision", zmux.ErrInvalidConfig)
	}
	return d.Liveness.Validate()
}

func (d *ConnectionDecision) ask(pol ConnectionPolicy, v *ConnectionView) (err error) {
	*d = ConnectionDecision{RetireConnections: d.RetireConnections[:0]}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("carrier: ConnectionPolicy.Decide panicked: %v", recovered)
		}
	}()
	if err := pol.Decide(v, d); err != nil {
		return err
	}
	return d.Validate()
}

func (d *ConnectionDecision) recheckAfter() time.Duration {
	const review = 100 * time.Millisecond
	if d.Recheck <= 0 {
		return review
	}
	return min(max(d.Recheck, 10*time.Millisecond), review)
}
