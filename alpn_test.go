package zmux

import "testing"

// The specification's table of ALPN aliases.
func TestALPNAlias(t *testing.T) {
	for _, tc := range []struct{ id, alias string }{
		{"qmux-02", "qmux-01"},
		{"qmux-02.moq-lite-04", "qmux-01.moq-lite-04"},
		{"h3qx-02", "h3qx-01"},
		{"myapp-02", "myapp-01"},
		{"myapp", ""},
		{"qmux-01", ""},
		{"QMUX-02", "QMUX-01"},
		{"qmux-02x", ""},
	} {
		a, ok := ALPNAlias(tc.id)
		if a != tc.alias || ok != (tc.alias != "") {
			t.Errorf("ALPNAlias(%q) = %q, %v; want %q", tc.id, a, ok, tc.alias)
		}
	}
}
