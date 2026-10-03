package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

const maxStdioToolRequests = 16

// Keep reading control frames while HTTP-backed tools run. The bound refuses
// excess work immediately, so a full tool pool cannot hide a cancellation.
// All responses still go through the session's sole stdout writer.
type stdioRequests struct {
	ctx    context.Context
	out    *stdioOutbound
	mu     sync.Mutex
	active map[string]context.CancelFunc
	err    error
	wg     sync.WaitGroup
}

func newStdioRequests(ctx context.Context, out *stdioOutbound) *stdioRequests {
	return &stdioRequests{ctx: ctx, out: out, active: make(map[string]context.CancelFunc)}
}

func stdioRequestKey(id any) string {
	value, _ := json.Marshal(id)
	return string(value)
}

func (calls *stdioRequests) Start(req jsonRPCRequest, server *Server) *jsonRPCResponse {
	key := stdioRequestKey(req.ID)
	calls.mu.Lock()
	refusal := ""
	if _, exists := calls.active[key]; exists {
		refusal = "Request ID is already active"
	} else if len(calls.active) >= maxStdioToolRequests {
		refusal = "Too many active tool requests; retry after an earlier request completes"
	}
	if refusal != "" {
		calls.mu.Unlock()
		return &jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: -32000, Message: refusal}}
	}
	ctx, cancel := context.WithCancel(calls.ctx)
	calls.active[key] = cancel
	calls.wg.Add(1)
	calls.mu.Unlock()
	go func() {
		defer calls.wg.Done()
		defer func() { cancel(); calls.mu.Lock(); delete(calls.active, key); calls.mu.Unlock() }()
		response := server.DispatchJSONRPC(ctx, &req)
		// Cancellation abandons this reply, but never retries the operation. A
		// signed write may already have reached consensus and keeps its normal
		// indeterminate-outcome/fence handling in the HTTP submission path.
		if response == nil || ctx.Err() != nil {
			return
		}
		if err := calls.out.WriteJSON(ctx, response); err != nil && ctx.Err() == nil {
			calls.mu.Lock()
			if calls.err == nil {
				calls.err = fmt.Errorf("SAGE MCP: write response: %w", err)
			}
			calls.mu.Unlock()
		}
	}()
	return nil
}

func (calls *stdioRequests) Cancel(params json.RawMessage) {
	var notice struct {
		RequestID any `json:"requestId"`
	}
	if json.Unmarshal(params, &notice) != nil || notice.RequestID == nil {
		return
	}
	calls.mu.Lock()
	if cancel := calls.active[stdioRequestKey(notice.RequestID)]; cancel != nil {
		cancel()
	}
	calls.mu.Unlock()
}

// Used before EOF completion or executable handoff. A replacement must never
// replay an already-dispatched request or share stdout with old tool workers.
func (calls *stdioRequests) Wait() error {
	calls.wg.Wait()
	calls.mu.Lock()
	defer calls.mu.Unlock()
	return calls.err
}

func (calls *stdioRequests) Close() {
	calls.mu.Lock()
	for _, cancel := range calls.active {
		cancel()
	}
	calls.mu.Unlock()
	_ = calls.Wait()
}
