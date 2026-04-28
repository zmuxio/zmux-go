package testutil

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

var (
	ndjsonCache    sync.Map
	jsonCache      sync.Map
	fixtureDirOnce sync.Once
	fixtureDir     string
)

func LocateFixtureDir(tb testing.TB) string {
	tb.Helper()

	fixtureDirOnce.Do(func() {
		fixtureDir = locateFixtureDir()
	})
	if fixtureDir != "" {
		return fixtureDir
	}
	tb.Skip("wire fixtures not found in testdata/fixtures")
	return ""
}

func locateFixtureDir() string {
	candidate := filepath.Join("testdata", "fixtures")
	info, err := os.Stat(candidate)
	if err != nil || !info.IsDir() {
		return ""
	}
	valid := filepath.Join(candidate, "wire_valid.ndjson")
	invalid := filepath.Join(candidate, "wire_invalid.ndjson")
	if _, err := os.Stat(valid); err != nil {
		return ""
	}
	if _, err := os.Stat(invalid); err != nil {
		return ""
	}
	return candidate
}

func LoadFixtureNDJSON[T any](tb testing.TB, name string) []T {
	tb.Helper()
	return LoadNDJSON[T](tb, filepath.Join(LocateFixtureDir(tb), name))
}

func LoadNDJSON[T any](tb testing.TB, path string) []T {
	tb.Helper()

	cacheKey := path + "::" + reflect.TypeOf((*T)(nil)).Elem().String()
	if cached, ok := ndjsonCache.Load(cacheKey); ok {
		return cloneJSONValue(tb, cached.([]T))
	}

	file, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open %s: %v", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	var fixtures []T
	decoder := json.NewDecoder(file)
	for {
		var fixture T
		if err := decoder.Decode(&fixture); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			tb.Fatalf("decode fixture entry in %s: %v", path, err)
		}
		fixtures = append(fixtures, fixture)
	}
	if len(fixtures) == 0 {
		tb.Fatalf("no fixtures loaded from %s", path)
	}
	ndjsonCache.Store(cacheKey, fixtures)
	return cloneJSONValue(tb, fixtures)
}

func ReadFixtureJSON[T any](tb testing.TB, name string) T {
	tb.Helper()
	return ReadJSON[T](tb, filepath.Join(LocateFixtureDir(tb), name))
}

func ReadJSON[T any](tb testing.TB, path string) T {
	tb.Helper()

	cacheKey := path + "::" + reflect.TypeOf((*T)(nil)).Elem().String()
	if cached, ok := jsonCache.Load(cacheKey); ok {
		return cloneJSONValue(tb, cached.(T))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("read %s: %v", path, err)
	}

	var file T
	if err := json.Unmarshal(data, &file); err != nil {
		tb.Fatalf("decode %s: %v", path, err)
	}
	jsonCache.Store(cacheKey, file)
	return cloneJSONValue(tb, file)
}

func cloneJSONValue[T any](tb testing.TB, in T) T {
	tb.Helper()

	data, err := json.Marshal(in)
	if err != nil {
		tb.Fatalf("clone fixture value: %v", err)
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		tb.Fatalf("decode cloned fixture value: %v", err)
	}
	return out
}
