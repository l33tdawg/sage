package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// stdioOutbound is the sole writer for one MCP stdio session. Responses and
// optional asynchronous host notifications share this pump so two valid JSON
// documents can never be interleaved on stdout.
type stdioOutbound struct {
	ctx    context.Context
	cancel context.CancelFunc
	frames chan stdioFrame
	done   chan struct{}

	errMu sync.RWMutex
	err   error
	once  sync.Once
}

type stdioFrame struct {
	payload []byte
	result  chan error
}

func newStdioOutbound(parent context.Context, writer io.Writer) *stdioOutbound {
	ctx, cancel := context.WithCancel(parent)
	out := &stdioOutbound{
		ctx:    ctx,
		cancel: cancel,
		frames: make(chan stdioFrame, 32),
		done:   make(chan struct{}),
	}
	go out.run(writer)
	return out
}

func (out *stdioOutbound) run(writer io.Writer) {
	defer close(out.done)
	for {
		select {
		case <-out.ctx.Done():
			return
		case frame := <-out.frames:
			line := append(frame.payload, '\n')
			written, err := writer.Write(line)
			if err == nil && written != len(line) {
				err = io.ErrShortWrite
			}
			if err != nil {
				out.setError(err)
			}
			frame.result <- err
			close(frame.result)
			if err != nil {
				return
			}
		}
	}
}

func (out *stdioOutbound) setError(err error) {
	out.errMu.Lock()
	if out.err == nil {
		out.err = err
	}
	out.errMu.Unlock()
}

func (out *stdioOutbound) terminalError() error {
	out.errMu.RLock()
	defer out.errMu.RUnlock()
	return out.err
}

func (out *stdioOutbound) WriteJSON(ctx context.Context, value any) error {
	if out == nil {
		return errors.New("MCP stdio writer is unavailable")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal MCP JSON-RPC frame: %w", err)
	}
	frame := stdioFrame{payload: payload, result: make(chan error, 1)}
	select {
	case out.frames <- frame:
	case <-out.done:
		if err := out.terminalError(); err != nil {
			return err
		}
		return errors.New("MCP stdio writer is closed")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-frame.result:
		return err
	case <-out.done:
		if err := out.terminalError(); err != nil {
			return err
		}
		return errors.New("MCP stdio writer closed before frame completion")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (out *stdioOutbound) Close() {
	if out == nil {
		return
	}
	out.once.Do(func() {
		out.cancel()
		<-out.done
	})
}
