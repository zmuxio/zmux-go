package zmux

// ListenerAdapter is the view of a Listener for the author of a carrier
// adapter or of an own accept loop: arrival slots, HTTP upgrade admission,
// source stall durations, join lookups, memory charging, setup-failure
// counting, and the listener's internal timeouts and clock. Listener.Adapter
// returns it; the application-facing methods stay on the Listener. It has
// no state of its own and is safe for concurrent use as the Listener is.
type ListenerAdapter Listener

// Adapter returns l's view for carrier adapters. A nil l gives a nil view,
// on which CountSetupFailure does nothing.
func (l *Listener) Adapter() *ListenerAdapter { return (*ListenerAdapter)(l) }

// ConnAdapter is the view of a Conn for the author of a carrier adapter or
// of a pool: what only code that dials carriers for a session needs.
// Conn.Adapter returns it.
type ConnAdapter Conn

// Adapter returns c's view for carrier adapters.
func (c *Conn) Adapter() *ConnAdapter { return (*ConnAdapter)(c) }
