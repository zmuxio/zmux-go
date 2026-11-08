package zmuxh3

import (
	"testing"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestMain fails the binary on goroutines the tests leave behind.
func TestMain(m *testing.M) { leakcheck.Main(m) }

// mustNewListener is zmux.NewListener for a configuration the test knows
// valid.
func mustNewListener(cfg *zmux.ListenerConfig) *zmux.Listener {
	l, err := zmux.NewListener(cfg)
	if err != nil {
		panic(err)
	}
	return l
}
