package wire

import "testing"

func TestLocatorOverLengthField(t *testing.T) {
	p := Params{Has: HasMultipath, Locator: make([]byte, 255)}
	if _, err := AppendParams(nil, &p); err != nil {
		t.Fatal(err)
	}
	p.Locator = make([]byte, 256)
	if b, err := AppendParams(nil, &p); err == nil || b != nil {
		t.Fatalf("%d bytes, %v", len(b), err)
	}
}
