package zmux

import "testing"

func TestPublicAPISymbolsRemainAvailable(t *testing.T) {
	t.Helper()

	_ = AsSession
	_ = NewSession
	_ = ClientSession
	_ = ServerSession
	_ = ErrAdapterUnsupported
}
