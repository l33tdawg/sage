package rest

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

type privateMediaResponse struct {
	Schema   string `json:"schema"`
	AgentID  string `json:"agent_id"`
	ObjectID string `json:"object_id"`
	Revision int64  `json:"revision"`
	Length   int64  `json:"length"`
	Digest   string `json:"digest"`
}

func (s *Server) SetPrivateMediaStore(media *store.PrivateMediaStore) {
	s.privateMedia.Store(media)
}

func privateMediaNoStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/private-media" || strings.HasPrefix(r.URL.Path, "/v1/private-media/") {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Content-Type-Options", "nosniff")
		}
		next.ServeHTTP(w, r)
	})
}

func privateMediaProblem(w http.ResponseWriter, status int) {
	writeProblem(w, status, "Private media unavailable", "The private media request could not be completed.")
}

func privateMediaError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, store.ErrPrivateMediaInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, store.ErrPrivateMediaNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrPrivateMediaConflict):
		status = http.StatusConflict
	case errors.Is(err, store.ErrPrivateMediaQuota):
		status = http.StatusTooManyRequests
	}
	privateMediaProblem(w, status)
}

func (s *Server) privateMediaContext(w http.ResponseWriter, r *http.Request) (*store.PrivateMediaStore, string, string, bool) {
	if !requireExactSignedMessageAction(w, r) {
		return nil, "", "", false
	}
	if !s.isPostV23ForNextTx() {
		privateMediaProblem(w, http.StatusServiceUnavailable)
		return nil, "", "", false
	}
	actor := middleware.ContextAgentID(r.Context())
	if actor == "" {
		privateMediaProblem(w, http.StatusForbidden)
		return nil, "", "", false
	}
	if !s.requireAppV23ActiveOrdinaryAgent(w, actor, "private media") {
		return nil, "", "", false
	}
	identifier := chi.URLParam(r, "uuid")
	parsed, err := uuid.Parse(identifier)
	if err != nil || parsed == uuid.Nil || parsed.String() != identifier || r.URL.RawQuery != "" || r.URL.EscapedPath() != r.URL.Path {
		privateMediaProblem(w, http.StatusBadRequest)
		return nil, "", "", false
	}
	media := s.privateMedia.Load()
	if media == nil {
		privateMediaProblem(w, http.StatusServiceUnavailable)
		return nil, "", "", false
	}
	w.Header().Set("X-SAGE-Private-Media-Schema", "sage.private-media.v1")
	return media, actor, identifier, true
}

func (s *Server) handlePutPrivateMedia(w http.ResponseWriter, r *http.Request) {
	media, actor, identifier, ok := s.privateMediaContext(w, r)
	if !ok {
		return
	}
	if len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "image/jpeg" || len(r.Header.Values("Content-Encoding")) != 0 {
		privateMediaProblem(w, http.StatusUnsupportedMediaType)
		return
	}
	image, err := io.ReadAll(http.MaxBytesReader(w, r.Body, store.MaxPrivateJPEGBytes))
	if err != nil {
		var oversized *http.MaxBytesError
		if errors.As(err, &oversized) {
			privateMediaProblem(w, http.StatusRequestEntityTooLarge)
		} else {
			privateMediaProblem(w, http.StatusBadRequest)
		}
		return
	}
	record, err := media.Put(r.Context(), actor, identifier, 0, image)
	if err != nil {
		privateMediaError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, privateMediaResponse{Schema: "sage.private-media.v1", AgentID: record.AgentID, ObjectID: record.ObjectID, Revision: record.Revision, Length: record.Length, Digest: record.Digest})
}

func (s *Server) handleGetPrivateMedia(w http.ResponseWriter, r *http.Request) {
	media, actor, identifier, ok := s.privateMediaContext(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1))
	if err != nil || len(body) != 0 {
		privateMediaProblem(w, http.StatusBadRequest)
		return
	}
	record, err := media.Get(r.Context(), actor, identifier)
	if err != nil {
		privateMediaError(w, err)
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("X-SAGE-Agent-ID", record.AgentID)
	w.Header().Set("X-SAGE-Media-ID", record.ObjectID)
	w.Header().Set("X-SAGE-Media-SHA256", record.Digest)
	w.Header().Set("Content-Length", strconv.Itoa(len(record.JPEG)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(record.JPEG)
}
