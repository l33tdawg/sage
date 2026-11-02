package abci

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	memoryTaintMirrorPassTimeout = 500 * time.Millisecond
	memoryTaintMirrorInterval    = time.Second
	memoryTaintMirrorStopTimeout = 2 * time.Second
)

// memoryTaintMirrorWorker is owned only by the committed SageApp. Speculative
// clones leave its pointer nil. It has no idle goroutine: requests coalesce into
// one running pass plus one dirty bit. I/O never runs under mu or on the trigger.
type memoryTaintMirrorWorker struct {
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	refresh     func(context.Context)
	interval    time.Duration
	passTimeout time.Duration
	lastStarted time.Time
	running     bool
	dirty       bool
	stopped     bool
	done        chan struct{}
}

func newMemoryTaintMirrorWorker(refresh func(context.Context)) *memoryTaintMirrorWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &memoryTaintMirrorWorker{
		ctx: ctx, cancel: cancel, refresh: refresh,
		interval: memoryTaintMirrorInterval, passTimeout: memoryTaintMirrorPassTimeout,
	}
}

func (w *memoryTaintMirrorWorker) request() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	w.dirty = true
	if w.running {
		return
	}
	w.running = true
	w.done = make(chan struct{})
	go w.run(w.done)
}

func (w *memoryTaintMirrorWorker) run(done chan struct{}) {
	for {
		w.mu.Lock()
		if w.stopped || !w.dirty {
			w.running = false
			close(done)
			w.mu.Unlock()
			return
		}
		wait := time.Until(w.lastStarted.Add(w.interval))
		w.mu.Unlock()
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-w.ctx.Done():
			}
			timer.Stop()
		}
		w.mu.Lock()
		if w.stopped {
			w.running = false
			close(done)
			w.mu.Unlock()
			return
		}
		// A request during the cooldown belongs to this pass. Requests arriving
		// during I/O retain one dirty bit for a later, rate-limited pass.
		w.dirty = false
		w.lastStarted = time.Now()
		w.mu.Unlock()
		ctx, cancel := context.WithTimeout(w.ctx, w.passTimeout)
		w.refresh(ctx)
		cancel()
	}
}

func (w *memoryTaintMirrorWorker) stop(timeout time.Duration) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	// Fence new generations first. Keep done after a timeout so every later
	// stop still joins the outstanding generation instead of claiming success.
	w.stopped = true
	w.dirty = false
	done := w.done
	w.mu.Unlock()
	w.cancel()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	default:
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return errors.New("advisory taint mirror worker did not stop; stores remain open")
	}
}

// startMemoryTaintMirrorWorker performs only runtime initialization and enqueue.
// Deadlines are cooperative; this does not promise zero shared I/O contention.
func (app *SageApp) startMemoryTaintMirrorWorker() {
	if app == nil || app.memoryTaintMirrorWorker != nil {
		return
	}
	mirror, ok := app.offchainStore.(store.MemoryTaintMirrorStore)
	if !ok || !store.MemoryTaintMirrorWorkerSupported(app.badgerStore, mirror) {
		return
	}
	// Capture committed store handles and logger, not the mutable app graph.
	canonical, logger := app.badgerStore, app.logger
	app.memoryTaintMirrorWorker = newMemoryTaintMirrorWorker(func(ctx context.Context) {
		revision, count, published, err := store.RefreshMemoryTaintMirrorBounded(
			ctx, canonical, mirror, store.DefaultMemoryTaintMirrorLimits(),
		)
		if err != nil {
			logger.Warn().Err(err).Msg("advisory taint mirror refresh deferred; serving remains Badger-authoritative")
			return
		}
		if published {
			logger.Info().Uint64("revision", revision).Int("tainted", count).Msg("rebuilt advisory taint mirror")
		}
	})
	app.requestMemoryTaintMirrorRefresh()
}

// A skipped, capped or failed pass retries only on the next constructor/Commit
// trigger (or a request already coalesced while it ran); there is no busy loop.
func (app *SageApp) requestMemoryTaintMirrorRefresh() {
	if app != nil {
		app.memoryTaintMirrorWorker.request()
	}
}

func (app *SageApp) stopMemoryTaintMirrorWorker() error {
	if app == nil {
		return nil
	}
	return app.memoryTaintMirrorWorker.stop(memoryTaintMirrorStopTimeout)
}
