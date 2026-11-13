// Package httpx holds what the HTTP carriers share with the root package:
// the request field names zmux defines, the rule for a field name a host
// chooses in their place (a pairing field, a prefix-authentication
// field), and a request body that replays bytes read before.
package httpx

import "strings"

const (
	// FieldPair pairs the uplink POST and the downlink GET of a split bootstrap.
	FieldPair = "Zmux-Pipe-Pair"
	// FieldPrefixAuth carries the header form of a prefix-authentication token.
	FieldPrefixAuth = "Zmux-Prefix-Auth"
)

// reserved are fields with a meaning of their own that a chosen field must
// not take over (lower case); prefixes are in reservedPrefixes.
var reserved = map[string]bool{
	"content-length": true, "content-type": true, "content-encoding": true, "transfer-encoding": true,
	"host": true, "connection": true, "upgrade": true, "keep-alive": true, "te": true, "trailer": true,
	"authorization": true, "cookie": true, "expect": true, "date": true, "early-data": true,
	"forwarded": true, "via": true, "location": true, "origin": true,
	// Fields that clients, browsers and the exchange adapter send on
	// ordinary requests: as a chosen field they would misclassify them.
	"cache-control": true, "pragma": true, "accept": true, "user-agent": true, "referer": true,
	"range": true,
}

var reservedPrefixes = []string{"proxy-", "sec-", "x-forwarded-", "accept-", "if-"}

// CheckField reports why field cannot be a field a host chooses: "" when it is
// empty or a header field name (an HTTP token) that no protocol field of
// HTTP, WebSocket or the adapters uses, FieldPrefixAuth included, and that
// ordinary requests do not carry; otherwise the reason, to follow the
// field's name in an error.
func CheckField(field string) string {
	for i := 0; i < len(field); i++ {
		c := field[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z',
			strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return "is not a header field name"
		}
	}
	l := strings.ToLower(field)
	r := reserved[l] || l == strings.ToLower(FieldPrefixAuth)
	for _, p := range reservedPrefixes {
		r = r || strings.HasPrefix(l, p)
	}
	if r {
		return "is reserved"
	}
	return ""
}
