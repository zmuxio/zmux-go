package zmuxhttp

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/zmuxio/zmux-go"
)

// CredentialHeader obtains a carrier credential's HTTP field and rejects an
// application field with the same name. The check also covers maps built
// with non-canonical field names.
func CredentialHeader(c zmux.CarrierCredential, info *zmux.CarrierInfo, fields http.Header) (string, string, error) {
	name, value, err := c.Header(info)
	if err != nil {
		return "", "", err
	}
	name = http.CanonicalHeaderKey(name)
	for field := range fields {
		if strings.EqualFold(field, name) {
			return "", "", fmt.Errorf("%w: carrier credential field %q is also a field of the request", zmux.ErrInvalidConfig, name)
		}
	}
	return name, value, nil
}
