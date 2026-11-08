package machine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// FormatError is an error of a shaping configuration. Class is one of
// "syntax", "duplicate_key", "unknown_member", "type", "range", "table",
// "scope", "conflict", "reference", "unreachable", "cycle", "zero_delay_cycle",
// "limit", and, from a bundle reader, "no_default" and "no_candidate".
type FormatError struct {
	Class string
	Msg   string
}

func (e *FormatError) Error() string { return "machine: " + e.Class + ": " + e.Msg }

func fail(class, format string, a ...any) *FormatError {
	return &FormatError{Class: class, Msg: fmt.Sprintf(format, a...)}
}

// jobject is a decoded JSON object; its values are nil, bool, string,
// json.Number, []any or *jobject.
type jobject struct {
	keys []string
	vals map[string]any
}

func (o *jobject) get(k string) any { return o.vals[k] }

// CheckSyntax checks the whole of a JSON document, parts a reader skips
// included: its syntax, one value, and no repeated name in an object.
func CheckSyntax(b []byte) error {
	_, err := decode(b)
	if err != nil {
		return err
	}
	return nil
}

func decode(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fail("syntax", "data after the value")
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	t, err := dec.Token()
	if err != nil {
		return nil, fail("syntax", "%v", err)
	}
	d, ok := t.(json.Delim)
	if !ok {
		return t, nil
	}
	if d == '{' {
		o := &jobject{vals: map[string]any{}}
		for dec.More() {
			k, err := dec.Token()
			if err != nil {
				return nil, fail("syntax", "%v", err)
			}
			name := k.(string)
			if _, dup := o.vals[name]; dup {
				return nil, fail("duplicate_key", "%q", name)
			}
			v, err := decodeValue(dec)
			if err != nil {
				return nil, err
			}
			o.keys = append(o.keys, name)
			o.vals[name] = v
		}
		if _, err := dec.Token(); err != nil {
			return nil, fail("syntax", "%v", err)
		}
		return o, nil
	}
	var a []any
	for dec.More() {
		v, err := decodeValue(dec)
		if err != nil {
			return nil, err
		}
		a = append(a, v)
	}
	if _, err := dec.Token(); err != nil {
		return nil, fail("syntax", "%v", err)
	}
	return a, nil
}

// members checks that x is an object of the allowed names and returns it; a
// null member counts as absent.
func members(x any, what string, allowed ...string) (*jobject, error) {
	o, ok := x.(*jobject)
	if !ok {
		return nil, fail("type", "%s: not an object", what)
	}
	for _, k := range o.keys {
		found := slices.Contains(allowed, k)
		if !found {
			return nil, fail("unknown_member", "%s.%s", what, k)
		}
	}
	return o, nil
}

// integer returns x as an integer of the integer form. A number outside
// int64 is reported in range.
func integer(x any, what string) (int64, error) {
	n, ok := x.(json.Number)
	if !ok || strings.ContainsAny(string(n), ".eE") {
		return 0, fail("type", "%s: not an integer", what)
	}
	v, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, fail("range", "%s: %s", what, n)
		}
		return 0, fail("type", "%s: %s", what, n)
	}
	return v, nil
}

func required(o *jobject, k, what string) (any, error) {
	v := o.get(k)
	if v == nil {
		return nil, fail("type", "%s.%s: missing", what, k)
	}
	return v, nil
}
