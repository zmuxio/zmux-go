package zmux

import "testing"

func TestConfigureDefaultConfigAffectsDefaultTemplate(t *testing.T) {
	ResetDefaultConfig()
	t.Cleanup(ResetDefaultConfig)

	ConfigureDefaultConfig(func(cfg *Config) {
		cfg.PrefacePadding = true
		cfg.PingPadding = true
		cfg.PingPaddingMinBytes = 33
		cfg.PingPaddingMaxBytes = 44
		cfg.TieBreakerNonce = 0x1234
		cfg.Settings = Settings{
			MaxControlPayloadBytes: 8192,
			PingPaddingKey:         0x99,
		}
	})

	cfg := DefaultConfig()
	if !cfg.PrefacePadding {
		t.Fatal("DefaultConfig().PrefacePadding = false, want true")
	}
	if !cfg.PingPadding {
		t.Fatal("DefaultConfig().PingPadding = false, want true")
	}
	if got := cfg.PingPaddingMinBytes; got != 33 {
		t.Fatalf("DefaultConfig().PingPaddingMinBytes = %d, want 33", got)
	}
	if got := cfg.PingPaddingMaxBytes; got != 44 {
		t.Fatalf("DefaultConfig().PingPaddingMaxBytes = %d, want 44", got)
	}
	if got := cfg.Settings.MaxControlPayloadBytes; got != 8192 {
		t.Fatalf("DefaultConfig().Settings.MaxControlPayloadBytes = %d, want 8192", got)
	}
	if got, want := cfg.Settings.MaxFramePayload, DefaultSettings().MaxFramePayload; got != want {
		t.Fatalf("DefaultConfig().Settings.MaxFramePayload = %d, want default %d", got, want)
	}
	if got := cfg.TieBreakerNonce; got != 0 {
		t.Fatalf("DefaultConfig().TieBreakerNonce = %#x, want zero template nonce", got)
	}
	if got := cfg.Settings.PingPaddingKey; got != 0 {
		t.Fatalf("DefaultConfig().Settings.PingPaddingKey = %#x, want zero template key", got)
	}

	cfg.PingPadding = false
	if !DefaultConfig().PingPadding {
		t.Fatal("DefaultConfig returned shared pointer; later copy lost PingPadding")
	}
	if !cloneConfig(nil).PingPadding {
		t.Fatal("cloneConfig(nil).PingPadding = false, want configured default")
	}
}

func TestResetDefaultConfigRestoresBuiltInTemplate(t *testing.T) {
	ResetDefaultConfig()
	t.Cleanup(ResetDefaultConfig)

	ConfigureDefaultConfig(func(cfg *Config) {
		cfg.PingPadding = true
	})
	if !DefaultConfig().PingPadding {
		t.Fatal("DefaultConfig().PingPadding = false after ConfigureDefaultConfig")
	}

	ResetDefaultConfig()
	cfg := DefaultConfig()
	if cfg.PingPadding {
		t.Fatal("DefaultConfig().PingPadding = true after ResetDefaultConfig")
	}
	if got, want := cfg.KeepaliveInterval, defaultIdleKeepaliveInterval; got != want {
		t.Fatalf("DefaultConfig().KeepaliveInterval = %v, want %v", got, want)
	}
}
