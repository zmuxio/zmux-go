package seal_test

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestMain fails the binary on goroutines the tests leave behind.
func TestMain(m *testing.M) { leakcheck.Main(m) }
