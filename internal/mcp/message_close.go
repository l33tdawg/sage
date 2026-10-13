package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/l33tdawg/sage/internal/store"
)

func (s *Server) toolMessageClose(ctx context.Context, params map[string]any) (any, error) {
	if err := s.requireBoundFederatedCaller(ctx); err != nil {
		return nil, err
	}
	messageID := stringParam(params, "message_id", "")
	reason := stringParam(params, "reason", "")
	if messageID == "" || strings.TrimSpace(reason) == "" || len(reason) > store.MaxMessageCloseReasonBytes {
		return nil, fmt.Errorf("'message_id' and a reason of 1-1024 bytes are required")
	}
	sessionID, err := s.claimantSessionID(ctx)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]any{"reason": reason, "claimant_session_id": sessionID})
	if err != nil {
		return nil, err
	}
	var response map[string]any
	if err := s.doSignedJSON(ctx, http.MethodPost, "/v1/messages/"+url.PathEscape(messageID)+"/close", body, &response); err != nil {
		return nil, fmt.Errorf("message close: %w", err)
	}
	response["message"] = "Message closed without a reply. It remains in history and no longer counts as unfinished inbox work."
	return response, nil
}
