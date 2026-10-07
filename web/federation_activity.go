package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

type federationActivityReader interface {
	RecentFederationActivity(context.Context) ([]store.FederationActivity, error)
}

// This dedicated stream has the federation operator gate, unlike the global
// payload-free invalidation bus. It never reads message content or claims work.
// Short connections re-enter session authentication on browser reconnection.
func (h *DashboardHandler) handleFedActivity(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Federation activity requires the local node operator.")
		return
	}
	reader, ok := h.store.(federationActivityReader)
	if !ok || h.BadgerStore == nil {
		fedWriteErr(w, http.StatusServiceUnavailable, "Federation activity is unavailable.")
		return
	}
	snapshot := func() ([]byte, error) {
		if !h.isFederationMutationOperatorRequest(r) {
			return nil, fmt.Errorf("operator session ended")
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		items, err := reader.RecentFederationActivity(ctx)
		if err != nil {
			return nil, err
		}
		agreements, err := h.BadgerStore.ListCrossFed()
		if err != nil {
			return nil, err
		}
		active := map[string]bool{}
		for _, a := range agreements {
			if a.Status == "active" && (a.ExpiresAt == 0 || a.ExpiresAt > time.Now().Unix()) {
				active[a.RemoteChainID] = true
			}
		}
		visible := make([]store.FederationActivity, 0, len(items))
		for _, item := range items {
			if active[item.ChainID] {
				visible = append(visible, item)
			}
		}
		return json.Marshal(map[string]any{"items": visible, "window_hours": 24, "limit": 100})
	}
	data, err := snapshot()
	if err != nil {
		fedWriteErr(w, http.StatusServiceUnavailable, "Federation activity could not be read.")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		fedWriteErr(w, http.StatusInternalServerError, "Streaming unavailable.")
		return
	}
	// Share the existing dashboard stream admission budget, with no data broadcast.
	if h.SSE == nil {
		fedWriteErr(w, http.StatusServiceUnavailable, "Streaming unavailable.")
		return
	}
	lease := h.SSE.Subscribe()
	if lease == nil {
		fedWriteErr(w, http.StatusServiceUnavailable, "Too many activity streams.")
		return
	}
	defer h.SSE.Unsubscribe(lease)
	w.Header().Set("Content-Type", "text/event-stream")
	rc := http.NewResponseController(w)
	send := func(body []byte) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := fmt.Fprintf(w, "event: federation_activity\ndata: %s\n\n", body); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if !send(data) {
		return
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	expiry := time.NewTimer(20 * time.Second)
	defer expiry.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-expiry.C:
			return
		case _, open := <-lease:
			if !open {
				return
			}
		case <-tick.C:
			next, err := snapshot()
			if err != nil {
				return
			}
			if string(next) != string(data) {
				if !send(next) {
					return
				}
				data = next
			} else {
				_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}
