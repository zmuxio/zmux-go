// Package testpeer runs subprocess fixtures and owns their stdout and exit.
package testpeer

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

// Command builds a command for an optional peer binary.
func Command(t *testing.T, env string, args ...string) *exec.Cmd {
	t.Helper()
	path := os.Getenv(env)
	if path == "" {
		t.Skipf("%s not set", env)
	}
	// The process owner controls cancellation during fixture cleanup.
	cmd := exec.CommandContext(context.WithoutCancel(t.Context()), path, args...) // #nosec G204 G702 -- the test harness supplies the fixture executable and arguments
	cmd.Stderr = os.Stderr
	return cmd
}

// Output reads peer status lines in arrival order and can hold other prefixes.
type Output struct {
	Lines <-chan string
	held  []string
}

// Next reads the next report. The timer only bounds a broken peer.
func (o *Output) Next(t *testing.T) string {
	t.Helper()
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	select {
	case line, ok := <-o.Lines:
		if !ok {
			t.Fatalf("peer output ended (held %q)", o.held)
		}
		return line
	case <-timer.C:
		t.Fatalf("peer produced no output (held %q)", o.held)
		return ""
	}
}

// ExpectNext requires a prefix on the next report.
func (o *Output) ExpectNext(t *testing.T, prefix string) string {
	t.Helper()
	line := o.Next(t)
	if rest, ok := strings.CutPrefix(line, prefix); ok {
		return strings.TrimSpace(rest)
	}
	t.Fatalf("peer: want %q, got %q", prefix, line)
	return ""
}

// Expect consumes the first matching report and saves intervening reports.
func (o *Output) Expect(t *testing.T, prefix string) string {
	t.Helper()
	for i, line := range o.held {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			o.held = slices.Delete(o.held, i, i+1)
			return strings.TrimSpace(rest)
		}
	}
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-o.Lines:
			if !ok {
				t.Fatalf("peer exited before %q (held %q)", prefix, o.held)
			}
			if rest, ok := strings.CutPrefix(line, prefix); ok {
				return strings.TrimSpace(rest)
			}
			o.held = append(o.held, line)
		case <-timer.C:
			t.Fatalf("peer produced no %q (held %q)", prefix, o.held)
		}
	}
}

// Process owns the child's output and exactly one call to Wait.
type Process struct {
	Output
	done chan struct{}
	err  error
}

// Start launches a peer and reaps it. When requireSuccess is false, a
// long-lived peer can be stopped at cleanup.
func Start(t *testing.T, cmd *exec.Cmd, requireSuccess bool) *Process {
	t.Helper()
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		_ = out.Close()
		t.Fatal(err)
	}
	lines := make(chan string, 16)
	p := &Process{Lines: lines, done: make(chan struct{})}
	go func() {
		scanner := bufio.NewScanner(out)
		var outputErr error
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			default:
				outputErr = errors.New("peer output queue exceeded its bound")
				_ = cmd.Process.Kill()
			}
			if outputErr != nil {
				break
			}
		}
		if outputErr == nil {
			outputErr = scanner.Err()
		}
		close(lines)
		_, _ = io.Copy(io.Discard, out)
		waitErr := cmd.Wait()
		if outputErr != nil {
			p.err = outputErr
		} else {
			p.err = waitErr
		}
		close(p.done)
	}()
	t.Cleanup(func() {
		if !requireSuccess || t.Failed() {
			_ = cmd.Process.Kill()
			_ = out.Close()
			<-p.done
			return
		}
		timer := time.NewTimer(time.Minute)
		defer timer.Stop()
		select {
		case <-p.done:
			if p.err != nil {
				t.Errorf("peer failed: %v", p.err)
			}
		case <-timer.C:
			_ = cmd.Process.Kill()
			_ = out.Close()
			<-p.done
			t.Error("peer did not terminate during cleanup")
		}
	})
	return p
}

// Wait waits for the single recorded process exit.
func (p *Process) Wait(t *testing.T) error {
	t.Helper()
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	select {
	case <-p.done:
		return p.err
	case <-timer.C:
		t.Fatal("peer did not exit")
		return nil
	}
}
