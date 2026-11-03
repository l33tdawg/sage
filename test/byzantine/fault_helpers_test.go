package byzantine

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

type statusClientFunc func(*http.Request) (*http.Response, error)

func (f statusClientFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestByzantineStatusRejectsUnavailableOrInvalidHeight(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
		want       int64
	}{
		{"valid", `{"result":{"sync_info":{"latest_block_height":"42","catching_up":false}}}`, 200, 42},
		{"http-error", `{}`, 503, 0},
		{"malformed", `{`, 200, 0},
		{"trailing-json", `{"result":{"sync_info":{"latest_block_height":"42","catching_up":false}}}{}`, 200, 0},
		{"missing-result", `{}`, 200, 0},
		{"missing-height", `{"result":{"sync_info":{"catching_up":false}}}`, 200, 0},
		{"missing-catching-up", `{"result":{"sync_info":{"latest_block_height":"42"}}}`, 200, 0},
		{"catching-up", `{"result":{"sync_info":{"latest_block_height":"42","catching_up":true}}}`, 200, 0},
		{"zero", `{"result":{"sync_info":{"latest_block_height":"0","catching_up":false}}}`, 200, 0},
		{"negative", `{"result":{"sync_info":{"latest_block_height":"-1","catching_up":false}}}`, 200, 0},
		{"noninteger", `{"result":{"sync_info":{"latest_block_height":"oops","catching_up":false}}}`, 200, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := statusClientFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || req.URL.String() != "http://fixture/status" {
					t.Fatalf("unexpected status request: %s %s", req.Method, req.URL)
				}
				return &http.Response{StatusCode: test.status, Body: io.NopCloser(strings.NewReader(test.body))}, nil
			})
			height, err := readBlockHeight(context.Background(), client, "http://fixture/")
			if (err == nil) != (test.want > 0) || height != test.want {
				t.Fatalf("height=%d error=%v, want height=%d", height, err, test.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := statusClientFunc(func(req *http.Request) (*http.Response, error) { return nil, req.Context().Err() })
	if _, err := readBlockHeight(ctx, client, "http://fixture"); !errors.Is(err, context.Canceled) {
		t.Fatalf("request lost cancellation: %v", err)
	}
}

func TestByzantineContainerControlRequiresSuccessAndActualState(t *testing.T) {
	for _, test := range []struct {
		name, state string
		running     bool
		failCommand int
		wantError   bool
	}{
		{"stopped", "false\n", false, 0, false},
		{"started", "true\n", true, 0, false},
		{"stop-failed", "false", false, 1, true},
		{"start-failed", "true", true, 1, true},
		{"inspect-failed", "false", false, 2, true},
		{"still-running", "true", false, 0, true},
		{"still-stopped", "false", true, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls [][]string
			run := func(_ context.Context, args ...string) ([]byte, error) {
				calls = append(calls, append([]string(nil), args...))
				if len(calls) == test.failCommand {
					return []byte("injected Docker failure"), errors.New("failed")
				}
				return []byte(test.state), nil
			}
			err := setContainerRunning(context.Background(), run, "sage-cometbft3-1", test.running)
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v, wantError=%t", err, test.wantError)
			}
			want := []string{"start", "sage-cometbft3-1"}
			if !test.running {
				want = []string{"stop", "--time", "3", "sage-cometbft3-1"}
			}
			if !reflect.DeepEqual(calls[0], want) {
				t.Fatalf("control command=%v, want %v", calls[0], want)
			}
			if test.failCommand == 1 && len(calls) != 1 {
				t.Fatal("inspected state after a failed Docker control command")
			}
		})
	}
}

func testFaultClock() (faultClock, *time.Time) {
	now := time.Unix(0, 0)
	return faultClock{now: func() time.Time { return now }, sleep: func(ctx context.Context, delay time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		now = now.Add(delay)
		return nil
	}}, &now
}

func TestByzantineHaltRequiresSettledAndContinuouslyUnchangedHeight(t *testing.T) {
	t.Run("settles-after-in-flight-commit", func(t *testing.T) {
		clock, now := testFaultClock()
		read := func(context.Context) (int64, error) {
			if now.Unix() == 0 {
				return 10, nil
			}
			return 11, nil
		}
		height, err := waitForStableHeight(context.Background(), read, clock, 3*time.Second, 15*time.Second)
		if err != nil || height != 11 || now.Unix() != 4 {
			t.Fatalf("settle height=%d at=%v error=%v", height, now, err)
		}
		if err := observeHeightHalt(context.Background(), read, clock, height, 12*time.Second); err != nil || now.Unix() != 16 {
			t.Fatalf("full observation at=%v error=%v", now, err)
		}
	})
	t.Run("cannot-settle-while-advancing", func(t *testing.T) {
		clock, now := testFaultClock()
		_, err := waitForStableHeight(context.Background(), func(context.Context) (int64, error) { return 10 + now.Unix(), nil }, clock, 3*time.Second, 15*time.Second)
		if err == nil || now.Unix() != 15 {
			t.Fatalf("advancing chain accepted or unbounded: at=%v error=%v", now, err)
		}
	})
	t.Run("late-advance-fails", func(t *testing.T) {
		clock, now := testFaultClock()
		err := observeHeightHalt(context.Background(), func(context.Context) (int64, error) {
			if now.Unix() >= 10 {
				return 11, nil
			}
			return 10, nil
		}, clock, 10, 12*time.Second)
		if err == nil || now.Unix() != 10 {
			t.Fatalf("late advancement accepted: at=%v error=%v", now, err)
		}
	})
	t.Run("unavailable-is-not-halted", func(t *testing.T) {
		clock, _ := testFaultClock()
		unavailable := errors.New("RPC unavailable")
		read := func(context.Context) (int64, error) { return 0, unavailable }
		if _, err := waitForStableHeight(context.Background(), read, clock, time.Second, 15*time.Second); !errors.Is(err, unavailable) {
			t.Fatalf("settle hid unavailable RPC: %v", err)
		}
		if err := observeHeightHalt(context.Background(), read, clock, 10, 12*time.Second); !errors.Is(err, unavailable) {
			t.Fatalf("halt hid unavailable RPC: %v", err)
		}
	})
}
