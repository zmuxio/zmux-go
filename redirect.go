package zmux

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
)

// Redirects of HTTP carriers: a carrier
// follows a redirect only to its own host, never to another host and never
// from https to http, so that no credential, pairing token, join frame or
// prefix reaches another origin.

// DefaultHTTPRedirects is the number of redirects an HTTP carrier request
// follows unless its client's CheckRedirect decides (or, for a WebSocket,
// its dial options set another bound).
const DefaultHTTPRedirects = 5

// HTTPRedirectError reports a redirect that an HTTP carrier did not
// follow: to another host, from https to http, one that would change a
// POST into a GET, past the bound, or without a valid Location.
type HTTPRedirectError struct {
	StatusCode int
	Location   string
	Reason     string
}

func (e *HTTPRedirectError) Error() string {
	return "zmux: HTTP carrier redirect " + strconv.Itoa(e.StatusCode) + " to " + strconv.Quote(e.Location) + " refused: " + e.Reason
}

// IsHTTPRedirect reports whether status is a redirect an HTTP carrier
// considers (301, 302, 303, 307, 308).
func IsHTTPRedirect(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	}
	return false
}

// CheckHTTPRedirect decides whether a carrier request with method to from,
// answered with the redirect status and location, is repeated at the URL
// it returns, with the same method, fields and body. It refuses, with an
// *HTTPRedirectError, a redirect to another host name, from a secure
// scheme (https, wss) to an insecure one, to another port without a
// scheme upgrade, and a 301, 302 or 303 of a request other than GET
// unless it is a pure scheme upgrade (the same host, path and query over
// https). The schemes ws and wss count as http and https; the result
// keeps the family of from. status must satisfy IsHTTPRedirect.
func CheckHTTPRedirect(from *url.URL, method string, status int, location string) (*url.URL, error) {
	refuse := func(reason string) (*url.URL, error) {
		return nil, &HTTPRedirectError{StatusCode: status, Location: location, Reason: reason}
	}
	if location == "" {
		return refuse("no Location")
	}
	loc, err := url.Parse(location)
	if err != nil {
		return refuse("invalid Location")
	}
	to := from.ResolveReference(loc)
	to.Fragment, to.RawFragment = "", ""
	fromTLS, ok := secureScheme(from.Scheme)
	if !ok {
		return refuse("unknown scheme " + from.Scheme)
	}
	toTLS, ok := secureScheme(to.Scheme)
	if !ok {
		return refuse("unknown scheme " + to.Scheme)
	}
	ws := from.Scheme == "ws" || from.Scheme == "wss"
	switch {
	case to.User != nil:
		return refuse("credentials in the Location")
	case !strings.EqualFold(to.Hostname(), from.Hostname()):
		return refuse("another host")
	case fromTLS && !toTLS:
		return refuse("downgrade to " + to.Scheme)
	case fromTLS == toTLS && effectivePort(to, toTLS) != effectivePort(from, fromTLS):
		return refuse("another port")
	}
	upgrade := !fromTLS && toTLS
	switch {
	case ws && toTLS:
		to.Scheme = "wss"
	case ws:
		to.Scheme = "ws"
	case toTLS:
		to.Scheme = "https"
	default:
		to.Scheme = "http"
	}
	if method != http.MethodGet && method != http.MethodHead && status != http.StatusTemporaryRedirect && status != http.StatusPermanentRedirect {
		// A 301, 302 or 303 turns a POST into a GET; only a pure scheme
		// upgrade is taken as meant for the same request.
		if !upgrade || to.EscapedPath() != from.EscapedPath() || to.RawQuery != from.RawQuery {
			return refuse("would change " + method + " to GET")
		}
	}
	return to, nil
}

// SchemeUpgradeOf reports whether to is from over a secure scheme: the
// same host, path and query, http to https (or ws to wss).
func SchemeUpgradeOf(from, to *url.URL) bool {
	f, _ := secureScheme(from.Scheme)
	t, _ := secureScheme(to.Scheme)
	return !f && t && strings.EqualFold(from.Hostname(), to.Hostname()) &&
		from.EscapedPath() == to.EscapedPath() && from.RawQuery == to.RawQuery
}

func secureScheme(s string) (secure, ok bool) {
	switch strings.ToLower(s) {
	case "https", "wss":
		return true, true
	case "http", "ws":
		return false, true
	}
	return false, false
}

func effectivePort(u *url.URL, secure bool) string {
	if p := u.Port(); p != "" {
		return p
	}
	if secure {
		return "443"
	}
	return "80"
}

// HTTPTarget is the URL of a carrier candidate and the URL that redirects
// last led it to: later carriers of the candidate start from the final
// URL, and DialMode remembers it per server in its CarrierCache. It is
// safe for concurrent use.
type HTTPTarget struct {
	base string
	cur  atomic.Pointer[string]
}

// NewHTTPTarget returns a target at rawURL.
func NewHTTPTarget(rawURL string) *HTTPTarget { return &HTTPTarget{base: rawURL} }

// URL is the URL the next carrier starts from.
func (t *HTTPTarget) URL() string {
	if p := t.cur.Load(); p != nil {
		return *p
	}
	return t.base
}

// Base is the URL the target was made with.
func (t *HTTPTarget) Base() string { return t.base }

// Redirected records the final URL of a carrier that redirects led to.
func (t *HTTPTarget) Redirected(final string) {
	if final == t.base {
		t.cur.Store(nil)
		return
	}
	t.cur.Store(&final)
}

// Reset starts the next carrier from the base URL again.
func (t *HTTPTarget) Reset() { t.cur.Store(nil) }

// noRedirectClient is client (nil: http.DefaultClient) with redirects
// returned to the carrier, which follows them itself.
func noRedirectClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &c
}

// errRedirectBound is the bound of hops when the client does not set one.
var errRedirectBound = errors.New("too many redirects")

// checkHop applies the client's CheckRedirect, or DefaultHTTPRedirects
// when it has none, to the next hop req after via.
func checkHop(client *http.Client, req *http.Request, via []*http.Request) error {
	if client != nil && client.CheckRedirect != nil {
		return client.CheckRedirect(req, via)
	}
	if len(via) > DefaultHTTPRedirects {
		return errRedirectBound
	}
	return nil
}

// doHTTP sends the request newReq builds for a URL, following redirects
// by CheckHTTPRedirect and checkHop: the last response (a non-redirect)
// and its URL. orig is the caller's client, nc its noRedirectClient;
// newReq builds a hop's request, and for a further hop gives up the one
// before.
func doHTTP(ctx context.Context, orig, nc *http.Client, u string, newReq func(u *url.URL) (*http.Request, error)) (*http.Response, *url.URL, error) {
	cur, err := url.Parse(u)
	if err != nil {
		return nil, nil, err
	}
	var via []*http.Request
	for {
		req, err := newReq(cur)
		if err != nil {
			if len(via) > 0 && errors.Is(err, errUpReplay) {
				err = &HTTPRedirectError{StatusCode: via[len(via)-1].Response.StatusCode,
					Location: cur.String(), Reason: err.Error()}
			}
			return nil, nil, err
		}
		if len(via) > 0 {
			if err := checkHop(orig, req, via); err != nil {
				if req.Body != nil {
					_ = req.Body.Close()
				}
				return nil, nil, &HTTPRedirectError{StatusCode: via[len(via)-1].Response.StatusCode,
					Location: cur.String(), Reason: err.Error()}
			}
		}
		resp, err := nc.Do(req)
		if err != nil {
			return nil, nil, err
		}
		if !IsHTTPRedirect(resp.StatusCode) {
			return resp, cur, nil
		}
		next, err := CheckHTTPRedirect(cur, req.Method, resp.StatusCode, resp.Header.Get("Location"))
		// The redirect's body is not the carrier's.
		_, _ = io.CopyN(io.Discard, resp.Body, 4<<10)
		_ = resp.Body.Close()
		if err != nil {
			return nil, nil, err
		}
		req.Response = resp
		via = append(via, req)
		cur = next
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
	}
}
