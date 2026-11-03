package byzantine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type statusClient interface {
	Do(*http.Request) (*http.Response, error)
}

func readBlockHeight(ctx context.Context, client statusClient, rpcURL string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(rpcURL, "/")+"/status", nil)
	if err != nil {
		return 0, fmt.Errorf("create status request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("read status: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status HTTP %d", resp.StatusCode)
	}
	var status struct {
		Result *struct {
			SyncInfo *struct {
				Height     string `json:"latest_block_height"`
				CatchingUp *bool  `json:"catching_up"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 64<<10))
	if decodeErr := decoder.Decode(&status); decodeErr != nil {
		return 0, fmt.Errorf("decode status: %w", decodeErr)
	}
	var trailing any
	if decodeErr := decoder.Decode(&trailing); !errors.Is(decodeErr, io.EOF) {
		return 0, fmt.Errorf("status contains trailing data")
	}
	if status.Result == nil || status.Result.SyncInfo == nil || status.Result.SyncInfo.CatchingUp == nil {
		return 0, fmt.Errorf("status is missing sync information")
	}
	if *status.Result.SyncInfo.CatchingUp {
		return 0, fmt.Errorf("status reports catching_up=true")
	}
	height, err := strconv.ParseInt(status.Result.SyncInfo.Height, 10, 64)
	if err != nil || height <= 0 {
		return 0, fmt.Errorf("status has invalid committed height %q", status.Result.SyncInfo.Height)
	}
	return height, nil
}

type dockerCommand func(context.Context, ...string) ([]byte, error)

func setContainerRunning(ctx context.Context, run dockerCommand, name string, running bool) error {
	args := []string{"start", name}
	if !running {
		args = []string{"stop", "--time", "3", name}
	}
	output, err := run(ctx, args...)
	if err != nil {
		return fmt.Errorf("docker %s %s: %w: %s", args[0], name, err, strings.TrimSpace(string(output)))
	}
	output, err = run(ctx, "inspect", "--format", "{{.State.Running}}", name)
	if err != nil {
		return fmt.Errorf("inspect container %s: %w: %s", name, err, strings.TrimSpace(string(output)))
	}
	if got, want := strings.TrimSpace(string(output)), strconv.FormatBool(running); got != want {
		return fmt.Errorf("container %s Running=%q, want %s", name, got, want)
	}
	return nil
}

type heightReader func(context.Context) (int64, error)

type faultClock struct {
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// Settle allows an already in-flight commit to finish after the two stops.
// The subsequent observation window must never allow another height change.
func waitForStableHeight(ctx context.Context, read heightReader, clock faultClock, stableFor, timeout time.Duration) (int64, error) {
	deadline := clock.now().Add(timeout)
	stableSince := clock.now()
	var previous int64
	for clock.now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		height, err := read(ctx)
		if err != nil {
			return 0, err
		}
		if height <= 0 || height < previous {
			return 0, fmt.Errorf("invalid or regressing height %d after %d", height, previous)
		}
		if height != previous {
			previous, stableSince = height, clock.now()
		}
		if clock.now().Sub(stableSince) >= stableFor {
			return height, nil
		}
		if sleepErr := clock.sleep(ctx, time.Second); sleepErr != nil {
			return 0, sleepErr
		}
	}
	return 0, fmt.Errorf("height did not settle for %s within %s", stableFor, timeout)
}

func observeHeightHalt(ctx context.Context, read heightReader, clock faultClock, height int64, window time.Duration) error {
	deadline := clock.now().Add(window)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		observed, err := read(ctx)
		if err != nil {
			return err
		}
		if observed != height {
			return fmt.Errorf("height changed during halt window: %d -> %d", height, observed)
		}
		if !clock.now().Before(deadline) {
			return nil
		}
		if sleepErr := clock.sleep(ctx, min(time.Second, deadline.Sub(clock.now()))); sleepErr != nil {
			return sleepErr
		}
	}
}
