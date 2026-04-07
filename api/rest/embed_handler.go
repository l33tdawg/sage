package rest

import (
	"net/http"
)

// EmbedRequest is the request body for POST /v1/embed.
type EmbedRequest struct {
	Text string `json:"text"`
}

// EmbedResponse is the response body for POST /v1/embed.
type EmbedResponse struct {
	Embedding []float32 `json:"embedding"`
	Model     string    `json:"model"`
	Dimension int       `json:"dimension"`
}

// EmbedInfoResponse describes the active embedding provider.
type EmbedInfoResponse struct {
	Semantic  bool   `json:"semantic"`
	Provider  string `json:"provider"`
	Dimension int    `json:"dimension"`
	Ready     bool   `json:"ready"`
}

// handleEmbedInfo returns metadata about the active embedding provider.
// Clients use this to decide between vector similarity and FTS5 text search.
func (s *Server) handleEmbedInfo(w http.ResponseWriter, r *http.Request) {
	provider := "hash"
	if s.embedder.Semantic() {
		provider = "ollama"
	}
	writeJSON(w, http.StatusOK, EmbedInfoResponse{
		Semantic:  s.embedder.Semantic(),
		Provider:  provider,
		Dimension: s.embedder.Dimension(),
		Ready:     s.embedder.Ready(),
	})
}

// handleEmbed generates a vector embedding via local Ollama.
// This allows agents to get embeddings from the SAGE network without
// running Ollama locally — fully sovereign, no cloud API calls.
func (s *Server) handleEmbed(w http.ResponseWriter, r *http.Request) {
	var req EmbedRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.Text == "" {
		writeProblem(w, http.StatusBadRequest, "Missing text", "text field is required.")
		return
	}

	emb, err := s.embedder.Embed(r.Context(), req.Text)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to generate embedding")
		writeProblem(w, http.StatusServiceUnavailable, "Embedding unavailable",
			"Failed to generate embedding. Ollama may not be ready.")
		return
	}

	writeJSON(w, http.StatusOK, EmbedResponse{
		Embedding: emb,
		Model:     "nomic-embed-text",
		Dimension: len(emb),
	})
}
