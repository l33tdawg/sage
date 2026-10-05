package rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

const maxWorkflowRequestBytes = store.MaxWorkflowJournalPayload + 512
const maxWorkflowJSONRevision int64 = 9007199254740991

type workflowJournalResponse struct {
	Schema   string          `json:"schema"`
	AgentID  string          `json:"agent_id"`
	RecordID string          `json:"record_id"`
	Revision int64           `json:"revision"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload"`
	Trust    string          `json:"trust"`
}

type workflowJournalPageResponse struct {
	Schema    string                    `json:"schema"`
	Items     []workflowJournalResponse `json:"items"`
	NextAfter *string                   `json:"next_after"`
	HasMore   bool                      `json:"has_more"`
}

func workflowJournalNoStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/workflows" || strings.HasPrefix(r.URL.Path, "/v1/workflows/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func workflowJournalProblem(w http.ResponseWriter, status int) {
	writeProblem(w, status, "Workflow journal unavailable", "The workflow journal request could not be completed.")
}

func workflowJournalError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, store.ErrWorkflowJournalInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, store.ErrWorkflowJournalNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrWorkflowJournalConflict):
		status = http.StatusConflict
	case errors.Is(err, store.ErrWorkflowJournalLimit):
		status = http.StatusTooManyRequests
	}
	workflowJournalProblem(w, status)
}

func (s *Server) workflowJournalContext(w http.ResponseWriter, r *http.Request) (*store.SQLiteStore, string, string, bool) {
	if !requireExactSignedMessageAction(w, r) {
		return nil, "", "", false
	}
	if !s.isPostV23ForNextTx() {
		workflowJournalProblem(w, http.StatusServiceUnavailable)
		return nil, "", "", false
	}
	actor := middleware.ContextAgentID(r.Context())
	if actor == "" {
		workflowJournalProblem(w, http.StatusForbidden)
		return nil, "", "", false
	}
	if !s.requireAppV23ActiveOrdinaryAgent(w, actor, "auxiliary workflow state") {
		return nil, "", "", false
	}
	identifier := chi.URLParam(r, "uuid")
	if r.URL.Path != "/v1/workflows" {
		parsed, err := uuid.Parse(identifier)
		if err != nil || parsed == uuid.Nil || parsed.String() != identifier || r.URL.RawQuery != "" {
			workflowJournalProblem(w, http.StatusBadRequest)
			return nil, "", "", false
		}
	}
	sqlite, supported := s.store.(*store.SQLiteStore)
	if !supported || sqlite == nil {
		workflowJournalProblem(w, http.StatusServiceUnavailable)
		return nil, "", "", false
	}
	return sqlite, actor, identifier, true
}

func workflowJournalView(record *store.WorkflowJournalRecord) workflowJournalResponse {
	return workflowJournalResponse{
		Schema: "sage.workflow-journal.v1", AgentID: record.AgentID, RecordID: record.RecordID,
		Revision: record.Revision, Kind: record.Kind, Payload: record.Payload, Trust: "untrusted_auxiliary",
	}
}

func respondWorkflowJournal(w http.ResponseWriter, record *store.WorkflowJournalRecord) {
	writeJSON(w, http.StatusOK, workflowJournalView(record))
}

func (s *Server) handleListWorkflowJournal(w http.ResponseWriter, r *http.Request) {
	sqlite, actor, _, ok := s.workflowJournalContext(w, r)
	if !ok {
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(r.URL.RawQuery) > 256 {
		workflowJournalProblem(w, http.StatusBadRequest)
		return
	}
	for name, values := range query {
		if (name != "after" && name != "limit") || len(values) != 1 {
			workflowJournalProblem(w, http.StatusBadRequest)
			return
		}
	}
	limit := 20
	if query.Has("limit") {
		limit, err = strconv.Atoi(query.Get("limit"))
		if err != nil || limit < 1 || limit > 50 || strconv.Itoa(limit) != query.Get("limit") {
			workflowJournalProblem(w, http.StatusBadRequest)
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1))
	if err != nil || len(body) != 0 {
		workflowJournalProblem(w, http.StatusBadRequest)
		return
	}
	page, err := sqlite.ListWorkflowJournal(r.Context(), actor, query.Get("after"), limit)
	if err != nil {
		workflowJournalError(w, err)
		return
	}
	response := workflowJournalPageResponse{Schema: "sage.workflow-journal.v1", Items: make([]workflowJournalResponse, 0, len(page.Records)), HasMore: page.More}
	for _, record := range page.Records {
		response.Items = append(response.Items, workflowJournalView(record))
	}
	if page.More {
		response.NextAfter = &page.NextCursor
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetWorkflowJournal(w http.ResponseWriter, r *http.Request) {
	sqlite, actor, identifier, ok := s.workflowJournalContext(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1))
	if err != nil || len(body) != 0 {
		workflowJournalProblem(w, http.StatusBadRequest)
		return
	}
	record, err := sqlite.GetWorkflowJournal(r.Context(), actor, identifier)
	if err != nil {
		workflowJournalError(w, err)
		return
	}
	respondWorkflowJournal(w, record)
}

func decodeWorkflowJournalObject(raw []byte, allowed ...string) (map[string]json.RawMessage, bool) {
	if !utf8.Valid(raw) {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, tokenErr := decoder.Token()
		name, text := token.(string)
		permitted := false
		for _, field := range allowed {
			permitted = permitted || name == field
		}
		if tokenErr != nil || !text || fields[name] != nil || !permitted {
			return nil, false
		}
		var value json.RawMessage
		if decodeErr := decoder.Decode(&value); decodeErr != nil {
			return nil, false
		}
		fields[name] = value
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return nil, false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return fields, true
}

func decodeWorkflowJournalRequest(raw []byte) (string, int64, json.RawMessage, *store.WorkflowJournalGuard, bool) {
	fields, valid := decodeWorkflowJournalObject(raw, "kind", "expected_revision", "payload", "guard")
	if !valid || fields["kind"] == nil || fields["expected_revision"] == nil || fields["payload"] == nil {
		return "", 0, nil, nil, false
	}
	var kind string
	if json.Unmarshal(fields["kind"], &kind) != nil || (kind != "mesh_outbound" && kind != "mesh_inbound" && kind != "public_proposal" &&
		kind != "conversation_control" && kind != "conversation_session") {
		return "", 0, nil, nil, false
	}
	revisionText := string(bytes.TrimSpace(fields["expected_revision"]))
	revision, err := strconv.ParseInt(revisionText, 10, 64)
	if err != nil || revision < 0 || revision >= maxWorkflowJSONRevision || strconv.FormatInt(revision, 10) != revisionText {
		return "", 0, nil, nil, false
	}
	var guard *store.WorkflowJournalGuard
	if rawGuard, supplied := fields["guard"]; supplied {
		condition, valid := decodeWorkflowJournalObject(rawGuard, "record_id", "expected_revision")
		if !valid || len(condition) != 2 || kind != "conversation_session" {
			return "", 0, nil, nil, false
		}
		var identifier string
		if json.Unmarshal(condition["record_id"], &identifier) != nil {
			return "", 0, nil, nil, false
		}
		parsed, err := uuid.Parse(identifier)
		if err != nil || parsed == uuid.Nil || parsed.String() != identifier {
			return "", 0, nil, nil, false
		}
		guardText := string(bytes.TrimSpace(condition["expected_revision"]))
		guardRevision, err := strconv.ParseInt(guardText, 10, 64)
		if err != nil || guardRevision < 1 || guardRevision > maxWorkflowJSONRevision || strconv.FormatInt(guardRevision, 10) != guardText {
			return "", 0, nil, nil, false
		}
		guard = &store.WorkflowJournalGuard{RecordID: identifier, ExpectedRevision: guardRevision}
	}
	return kind, revision, fields["payload"], guard, true
}

func (s *Server) handlePutWorkflowJournal(w http.ResponseWriter, r *http.Request) {
	sqlite, actor, identifier, ok := s.workflowJournalContext(w, r)
	if !ok {
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxWorkflowRequestBytes+1))
	if err != nil {
		workflowJournalProblem(w, http.StatusBadRequest)
		return
	}
	if len(raw) > maxWorkflowRequestBytes {
		workflowJournalProblem(w, http.StatusRequestEntityTooLarge)
		return
	}
	kind, revision, payload, guard, valid := decodeWorkflowJournalRequest(raw)
	if !valid {
		workflowJournalProblem(w, http.StatusBadRequest)
		return
	}
	if len(payload) > store.MaxWorkflowJournalPayload {
		workflowJournalProblem(w, http.StatusRequestEntityTooLarge)
		return
	}
	record, err := sqlite.PutWorkflowJournalGuarded(r.Context(), actor, identifier, kind, payload, revision, guard)
	if err != nil {
		workflowJournalError(w, err)
		return
	}
	respondWorkflowJournal(w, record)
}
