package web

import (
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

// Memory "train of thought" - the related-memories endpoint that powers the MRI
// click-to-explore. Given a clicked memory, it returns the top-K most-related
// memories ranked by a blend of signals that need NO semantic embeddings:
//
//   - chain lineage (parent via parent_hash)               - strongest
//   - shared tags (same explicit topic)                    - strong
//   - full-text content similarity (per-word FTS overlap)  - strong, but only on
//     an UNencrypted node (SearchByText fails on an encrypted vault; best-effort)
//   - same domain (same lobe), high-confidence first       - filler, low weight
//
// Metadata signals (chain/tags/domain) work on an encrypted node too, so the
// feature degrades gracefully rather than erroring.

const (
	relatedDefaultK = 50
	relatedMaxK     = 120
	relatedFTSWords = 8  // distinct content words to OR-search
	relatedPerTerm  = 12 // FTS hits per word
)

// RelatedMemory is one node in a memory's train of thought.
type RelatedMemory struct {
	ID                 string  `json:"id"`
	Content            string  `json:"content"`
	Domain             string  `json:"domain"`
	Confidence         float64 `json:"confidence"`
	CorroborationCount int     `json:"corroboration_count"`
	Status             string  `json:"status"`
	CreatedAt          string  `json:"created_at"`
	MemoryType         string  `json:"memory_type"`
	Kind               string  `json:"kind"`     // do | dont | observation | note (for the board columns)
	Relation           string  `json:"relation"` // chain | same-topic | similar | same-lobe
	Score              float64 `json:"score"`
}

// classifyKind buckets a memory for the train-of-thought board. A [DO]/[DON'T]
// content prefix (from reflections) wins over memory_type; observations get
// their own column; everything else (facts/inferences/tasks) is a note. [DON'T
// is checked before [DO so "[DON'T]" isn't mistaken for a Do.
func classifyKind(content, memType string) string {
	c := strings.ToUpper(strings.TrimSpace(content))
	switch {
	case strings.HasPrefix(c, "[DON"):
		return "dont"
	case strings.HasPrefix(c, "[DO"):
		return "do"
	case memType == "observation":
		return "observation"
	default:
		return "note"
	}
}

var relatedWordRe = regexp.MustCompile(`[a-z0-9]{4,}`)

// relatedStop are common words that carry no topical signal.
var relatedStop = map[string]bool{
	"this": true, "that": true, "with": true, "from": true, "have": true, "will": true,
	"they": true, "them": true, "then": true, "than": true, "your": true, "what": true,
	"when": true, "which": true, "into": true, "some": true, "more": true, "very": true,
	"just": true, "also": true, "been": true, "were": true, "each": true, "such": true,
	"only": true, "over": true, "here": true, "there": true, "about": true, "would": true,
	"could": true, "should": true, "these": true, "those": true, "their": true, "other": true,
}

func relationRank(rel string) int {
	switch rel {
	case "chain":
		return 4
	case "same-topic":
		return 3
	case "similar":
		return 2
	case "same-lobe":
		return 1
	}
	return 0
}

// handleMemoryRelated handles GET /v1/dashboard/memory/{id}/related?k=50.
func (h *DashboardHandler) handleMemoryRelated(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing memory id")
		return
	}
	k := relatedDefaultK
	if v, _ := strconv.Atoi(r.URL.Query().Get("k")); v > 0 {
		k = v
	}
	if k > relatedMaxK {
		k = relatedMaxK
	}
	ctx := r.Context()
	x, err := h.store.GetMemory(ctx, id)
	if err != nil || x == nil {
		writeError(w, http.StatusNotFound, "memory not found")
		return
	}

	// On-chain RBAC: an MCP agent request is restricted to its visible agents;
	// the operator dashboard (cookie session, no X-Agent-ID) sees all.
	allowedAgents, seeAll := h.resolveAgentRBAC(r)
	allowed := make(map[string]bool, len(allowedAgents))
	for _, a := range allowedAgents {
		allowed[a] = true
	}

	// IDOR guard: the related set is already visibility-filtered in bump(), but the
	// ANCHOR memory's own content/domain is returned below - so an MCP agent that
	// cannot see this memory's submitter must be refused here too. Mirror the
	// "not found" response so the endpoint does not leak the memory's existence.
	if !seeAll && !allowed[x.SubmittingAgent] {
		writeError(w, http.StatusNotFound, "memory not found")
		return
	}

	type acc struct {
		rec      *memory.MemoryRecord
		score    float64
		relation string
	}
	pool := make(map[string]*acc)
	bump := func(rec *memory.MemoryRecord, add float64, relation string) {
		if rec == nil || rec.MemoryID == id {
			return
		}
		if !seeAll && !allowed[rec.SubmittingAgent] {
			return
		}
		e := pool[rec.MemoryID]
		if e == nil {
			e = &acc{rec: rec}
			pool[rec.MemoryID] = e
		}
		e.score += add
		if relationRank(relation) > relationRank(e.relation) {
			e.relation = relation
		}
	}

	// 1. Chain: the parent memory (direct lineage).
	if x.ParentHash != "" {
		if p, pErr := h.store.GetMemory(ctx, x.ParentHash); pErr == nil {
			bump(p, 6.0, "chain")
		}
	}

	// 2. Shared tags (same explicit topic).
	if tags, tErr := h.store.GetTags(ctx, id); tErr == nil {
		for _, tag := range tags {
			recs, _, _ := h.store.ListMemoriesByTag(ctx, tag, 25, 0)
			for _, rec := range recs {
				bump(rec, 2.0, "same-topic")
			}
		}
	}

	// 3. Content similarity. Prefer the FTS index (fast) when available; on an
	// encrypted vault the FTS index is disabled, so fall back to computing
	// keyword overlap in-process over a candidate pool - GetMemory/ListMemories
	// return DECRYPTED content when the vault is unlocked, so this still works.
	xWords := wordSet(x.Content)
	ftsOK := len(xWords) > 0
	if ftsOK {
		for _, word := range relatedContentWords(x.Content) {
			hits, sErr := h.store.SearchByText(ctx, word, store.QueryOptions{TopK: relatedPerTerm})
			if sErr != nil {
				ftsOK = false // encrypted vault / FTS off - switch to in-process overlap
				break
			}
			for i, rec := range hits {
				bump(rec, 3.0/float64(i+1), "similar")
			}
		}
	}
	if !ftsOK && len(xWords) > 0 {
		// Candidate pool: same lobe (topical) + most recent (temporal train of
		// thought), deduped. Score by shared significant-word overlap.
		seenCand := make(map[string]bool)
		scoreOverlap := func(recs []*memory.MemoryRecord) {
			for _, rec := range recs {
				if rec.MemoryID == id || seenCand[rec.MemoryID] {
					continue
				}
				seenCand[rec.MemoryID] = true
				if ov := overlapCount(xWords, wordSet(rec.Content)); ov > 0 {
					bump(rec, float64(ov)*1.3, "similar")
				}
			}
		}
		sameLobe, _, _ := h.store.ListMemories(ctx, store.ListOptions{
			DomainTag: x.DomainTag, Sort: "newest", Limit: 250, Status: "committed",
		})
		scoreOverlap(sameLobe)
		recent, _, _ := h.store.ListMemories(ctx, store.ListOptions{
			Sort: "newest", Limit: 250, Status: "committed",
		})
		scoreOverlap(recent)
	}

	// 4. Same-lobe filler (low weight), so the panel is never empty even for a
	// tag-less, parent-less memory whose content shares no vocabulary.
	if len(pool) < k {
		recs, _, _ := h.store.ListMemories(ctx, store.ListOptions{
			DomainTag: x.DomainTag, Sort: "confidence", Limit: k, Status: "committed",
		})
		for _, rec := range recs {
			bump(rec, 0.25, "same-lobe")
		}
	}

	items := make([]*acc, 0, len(pool))
	for _, e := range pool {
		items = append(items, e)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		return items[i].rec.MemoryID < items[j].rec.MemoryID // stable
	})
	if len(items) > k {
		items = items[:k]
	}

	ids := make([]string, len(items))
	for i, e := range items {
		ids[i] = e.rec.MemoryID
	}
	corr, _ := h.store.GetCorroborationCounts(ctx, ids)

	related := make([]RelatedMemory, 0, len(items))
	for _, e := range items {
		related = append(related, RelatedMemory{
			ID:                 e.rec.MemoryID,
			Content:            truncate(e.rec.Content, 160),
			Domain:             e.rec.DomainTag,
			Confidence:         e.rec.ConfidenceScore,
			CorroborationCount: corr[e.rec.MemoryID],
			Status:             string(e.rec.Status),
			CreatedAt:          e.rec.CreatedAt.Format(time.RFC3339),
			MemoryType:         string(e.rec.MemoryType),
			Kind:               classifyKind(e.rec.Content, string(e.rec.MemoryType)),
			Relation:           e.relation,
			Score:              e.score,
		})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{
		"id":      id,
		"domain":  x.DomainTag,
		"content": truncate(x.Content, 160),
		"related": related,
	})
}

// wordSet tokenizes content into a set of significant words (lowercased, >=4
// chars, minus stopwords) for in-process overlap scoring.
func wordSet(content string) map[string]bool {
	out := make(map[string]bool)
	for _, wd := range relatedWordRe.FindAllString(strings.ToLower(content), -1) {
		if !relatedStop[wd] {
			out[wd] = true
		}
	}
	return out
}

// overlapCount returns how many significant words two memories share.
func overlapCount(a, b map[string]bool) int {
	// iterate the smaller set
	if len(b) < len(a) {
		a, b = b, a
	}
	n := 0
	for w := range a {
		if b[w] {
			n++
		}
	}
	return n
}

// relatedContentWords extracts up to relatedFTSWords distinct significant words
// from a memory's content (lowercased, >=4 chars, minus stopwords) for the FTS
// overlap search.
func relatedContentWords(content string) []string {
	words := relatedWordRe.FindAllString(strings.ToLower(content), -1)
	seen := make(map[string]bool)
	out := make([]string, 0, relatedFTSWords)
	for _, wd := range words {
		if relatedStop[wd] || seen[wd] {
			continue
		}
		seen[wd] = true
		out = append(out, wd)
		if len(out) >= relatedFTSWords {
			break
		}
	}
	return out
}
