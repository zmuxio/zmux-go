package zmux_test

import "github.com/zmuxio/zmux-go"

// mustNewListener is zmux.NewListener for a configuration the test knows
// valid.
func mustNewListener(cfg *zmux.ListenerConfig) *zmux.Listener {
	l, err := zmux.NewListener(cfg)
	if err != nil {
		panic(err)
	}
	return l
}
