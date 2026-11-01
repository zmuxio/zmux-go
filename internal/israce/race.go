//go:build race

// Package israce reports whether the race detector is on, for tests whose
// allocation counts it changes.
package israce

// On is true under the race detector.
const On = true
