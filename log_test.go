package fsm

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// logCapture is a race-safe sink for a slog handler: the run loop, the coordinate loop and the
// bolt queue runner all write from their own goroutines.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *logCapture) lines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Split(strings.TrimSpace(c.buf.String()), "\n")
}

// runLogged drives a two-transition run on a manager whose logger writes to the capture at the
// given level, and returns the captured lines once the manager has shut down, so no goroutine is
// still writing.
func runLogged(t *testing.T, b *backend, level slog.Level) []string {
	t.Helper()
	ctx := context.Background()

	capture := &logCapture{}
	b.configureManager = func(cfg *Config) {
		cfg.Logger = slog.New(slog.NewTextHandler(capture, &slog.HandlerOptions{Level: level}))
	}
	m, stop := b.newManager(nil)

	pass := func(context.Context, *Request[orderReq, orderResp]) (*Response[orderResp], error) {
		return nil, nil
	}
	start, _, err := m.Register[orderReq, orderResp]("logged").
		Start("first", pass).
		To("second", pass).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version, err := start(ctx, "logged-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.Wait(waitCtx, version); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	stop()
	return capture.lines()
}

func linesWithMessage(lines []string, msg string) []string {
	var matched []string
	for _, line := range lines {
		if strings.Contains(line, `msg="`+msg+`"`) {
			matched = append(matched, line)
		}
	}
	return matched
}

// TestTransitionLogsAreDebug pins the level policy: a consumer logging at Info sees a run start
// and stop but not every transition; one logging at Debug sees the transitions, and each
// transition line carries the run's fsm group — keyed like its span and metric attributes —
// exactly once.
func TestTransitionLogsAreDebug(t *testing.T) { runBackends(t, testTransitionLogsAreDebug) }

func testTransitionLogsAreDebug(t *testing.T, b *backend) {
	info := runLogged(t, b, slog.LevelInfo)
	if len(linesWithMessage(info, "starting fsm")) != 1 {
		t.Fatalf("expected one 'starting fsm' line at Info, got:\n%s", strings.Join(info, "\n"))
	}
	for _, msg := range []string{"running transition", "transition completed successfully", "waiting for FSM to finish"} {
		if lines := linesWithMessage(info, msg); len(lines) != 0 {
			t.Fatalf("expected no %q lines at Info, got:\n%s", msg, strings.Join(lines, "\n"))
		}
	}

	debug := runLogged(t, b, slog.LevelDebug)
	running := linesWithMessage(debug, "running transition")
	if len(running) != 3 { // first, second, done
		t.Fatalf("expected a 'running transition' line per transition at Debug, got %d:\n%s", len(running), strings.Join(running, "\n"))
	}
	for _, line := range running {
		for _, attr := range []string{"sys=fsm", "fsm.action=logged", "fsm.id=logged-1", "fsm.alias=order_req", "fsm.version="} {
			if !strings.Contains(line, " "+attr) {
				t.Fatalf("expected %s on a transition line, got: %s", attr, line)
			}
		}
		if n := strings.Count(line, " fsm.state="); n != 1 {
			t.Fatalf("expected exactly one fsm.state per line, got %d: %s", n, line)
		}
	}
}
