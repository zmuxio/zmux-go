package wire

import "testing"

func TestMarshalPrefaceWithSettingsPaddingRoundTrip(t *testing.T) {
	t.Parallel()

	p := Preface{
		PrefaceVersion: PrefaceVersion,
		Role:           RoleInitiator,
		MinProto:       ProtoVersion,
		MaxProto:       ProtoVersion,
		Settings:       DefaultSettings(),
	}
	base, err := p.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary err = %v", err)
	}
	padded, err := MarshalPrefaceWithSettingsPadding(p, []byte{0xff, 0x00, 0x01})
	if err != nil {
		t.Fatalf("MarshalPrefaceWithSettingsPadding err = %v", err)
	}
	if len(padded) <= len(base) {
		t.Fatalf("padded preface len = %d, want > base len %d", len(padded), len(base))
	}

	parsed, err := ParsePreface(padded)
	if err != nil {
		t.Fatalf("ParsePreface padded err = %v", err)
	}
	if parsed != p {
		t.Fatalf("ParsePreface padded = %+v, want %+v", parsed, p)
	}
}
