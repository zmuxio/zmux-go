package machine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// The strict JSON layer: member names match exactly, an object holds a
// name once, unknown members are errors, null is an absent member, integer
// members are written as JSON integers and numbers fit in a float64. It
// checks a document before encoding/json decodes it into the Go types,
// which then sees exact names only.

// jobject is a decoded JSON object; its values are nil, bool, string,
// json.Number, []any or *jobject.
type jobject struct {
	keys []string
	vals map[string]any
}

// decodeStrict decodes one JSON value from b.
func decodeStrict(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("parse: data after the value")
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	t, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	switch t := t.(type) {
	case json.Delim:
		switch t {
		case '{':
			o := &jobject{vals: map[string]any{}}
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("parse: %w", err)
				}
				name := k.(string)
				if _, dup := o.vals[name]; dup {
					return nil, fmt.Errorf("duplicate field %q", name)
				}
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				o.keys = append(o.keys, name)
				o.vals[name] = v
			}
			_, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("parse: %w", err)
			}
			return o, nil
		case '[':
			a := []any{}
			for dec.More() {
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, fmt.Errorf("parse: %w", err)
			}
			return a, nil
		}
	case json.Number:
		if _, err := strconv.ParseFloat(string(t), 64); err != nil {
			return nil, fmt.Errorf("number %s: overflow", t)
		}
	}
	return t, nil
}

// check validates a decoded value.
type check func(v any) error

// member describes an object member: its check (nil: any value) and
// whether it is required (present and not null).
type member struct {
	check    check
	required bool
}

// object checks an object with the members ms.
func object(ms map[string]member) check {
	return func(v any) error { return objectOf(v, ms, nil) }
}

// objectOf checks v, an object with the members ms; allowed, when not nil,
// says which of them may be other than null.
func objectOf(v any, ms map[string]member, allowed func(string) bool) error {
	o, ok := v.(*jobject)
	if !ok {
		return errors.New("not an object")
	}
	for _, k := range o.keys {
		m, ok := ms[k]
		if !ok || o.vals[k] != nil && allowed != nil && !allowed(k) {
			return fmt.Errorf("unknown field %q", k)
		}
		if m.check != nil && o.vals[k] != nil {
			if err := m.check(o.vals[k]); err != nil {
				return fmt.Errorf("%s: %w", k, err)
			}
		}
	}
	for k, m := range ms {
		if m.required && o.vals[k] == nil {
			return fmt.Errorf("missing field %q", k)
		}
	}
	return nil
}

func arrayOf(c check) check {
	return func(v any) error {
		a, ok := v.([]any)
		if !ok {
			return nil // left to encoding/json
		}
		for i, x := range a {
			if x == nil {
				return fmt.Errorf("%d: null", i)
			}
			if err := c(x); err != nil {
				return fmt.Errorf("%d: %w", i, err)
			}
		}
		return nil
	}
}

// integer checks the lexical form of an integer member.
func integer(v any) error {
	if n, ok := v.(json.Number); ok && strings.ContainsAny(string(n), ".eE") {
		return fmt.Errorf("%s is not an integer", n)
	}
	return nil
}

// scopeName rejects the empty scope, which the Go type takes as each.
func scopeName(v any) error {
	if s, ok := v.(string); ok && s == "" {
		return errors.New(`unknown scope ""`)
	}
	return nil
}

var valueMembers, distMembers map[string]member

func init() {
	valueMembers = map[string]member{
		"const": {}, "range": {}, "dist": {check: distShape},
		"scope": {check: scopeName}, "min": {}, "max": {},
	}
	distMembers = map[string]member{
		"kind": {required: true}, "params": {}, "points": {}, "weights": {}, "of": {check: arrayOf(valueShape)},
	}
}

func valueShape(v any) error { return objectOf(v, valueMembers, nil) }

// distShape checks a distribution: members other than kind as its kind
// uses them.
func distShape(v any) error {
	var kind DistKind
	if o, ok := v.(*jobject); ok {
		s, _ := o.vals["kind"].(string)
		kind = DistKind(s)
	}
	allowed := func(k string) bool {
		switch kind {
		case DistEmpirical:
			return k == "kind" || k == "points" || k == "weights"
		case DistBag:
			return k == "kind" || k == "points"
		case DistMixture:
			return k == "kind" || k == "of" || k == "weights"
		}
		if _, known := nParams[kind]; !known {
			return true // left to Validate
		}
		return k == "kind" || k == "params"
	}
	return objectOf(v, distMembers, allowed)
}

var actionMembers = map[string]member{
	"op": {required: true}, "cancel": {}, "slot": {check: integer},
	"after": {check: valueShape}, "len": {check: valueShape}, "rate": {check: valueShape},
	"burst": {check: valueShape}, "value": {check: valueShape},
}

// opMembers lists the members each op may set.
var opMembers = map[Op]string{
	OpInject: "cancel after len", OpPause: "after", OpRate: "rate burst",
	OpTimer: "cancel slot after", OpSet: "slot value", OpAdd: "slot value", OpSub: "slot value",
	OpWeight: "value", OpStop: "", OpRestart: "",
}

func actionShape(v any) error {
	var op Op
	if o, ok := v.(*jobject); ok {
		s, _ := o.vals["op"].(string)
		op = Op(s)
	}
	use, known := opMembers[op]
	allowed := func(k string) bool {
		if !known || k == "op" {
			return true // an unknown op is left to Validate
		}
		for _, f := range strings.Fields(use) {
			if f == k {
				return true
			}
		}
		return false
	}
	return objectOf(v, actionMembers, allowed)
}

var definitionShape = object(map[string]member{
	"version": {check: integer, required: true},
	"machines": {check: arrayOf(object(map[string]member{
		"start": {check: integer},
		"states": {check: arrayOf(object(map[string]member{
			"record": {check: object(map[string]member{
				"max_len": {check: valueShape}, "pad_to": {check: valueShape},
				"pad_multiple": {check: valueShape}, "pad": {check: valueShape},
				"delay": {check: valueShape}, "flush": {},
			})},
			"enter": {check: arrayOf(actionShape)},
			"next": {check: arrayOf(object(map[string]member{
				"on": {required: true}, "to": {required: true, check: integer},
				"prob": {check: valueShape},
				"if": {check: object(map[string]member{
					"what": {required: true}, "cmp": {required: true},
					"value": {required: true, check: valueShape},
				})},
			}))},
		}))},
	}))},
})

var poolShape = object(map[string]member{
	"count": {check: valueShape}, "dial_after": {check: arrayOf(valueShape)}, "dial_jitter": {},
	"retire": {check: arrayOf(object(map[string]member{
		"metric": {required: true}, "above": {required: true, check: valueShape}, "jitter": {},
	}))},
	"probe_interval": {check: valueShape}, "probe_jitter": {},
})

var limitsShape = object(map[string]member{
	"max_padding_bytes": {check: integer}, "max_padding_ratio": {}, "max_injected": {check: integer},
	"max_pause_total": {}, "max_delay": {},
})

// decodeChecked checks b by shape and decodes it into out.
func decodeChecked(b []byte, shape check, out any) error {
	v, err := decodeStrict(b)
	if err != nil {
		return err
	}
	if err := shape(v); err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
