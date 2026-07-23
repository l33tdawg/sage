package federation

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestValidFederatedRecallResultRejectsContractAndHashSpoofing(t *testing.T) {
	content := "authenticated but still untrusted"
	sum := sha256.Sum256([]byte(content))
	req := &QueryRequest{DomainTag: "sage-autoresearch-benchmark"}
	base := &MemoryResult{
		MemoryID: "remote-1", Content: content, ContentHash: hex.EncodeToString(sum[:]),
		DomainTag: "sage-autoresearch-benchmark", ConfidenceScore: .8,
		Classification: 1, Status: "committed",
	}
	if !validFederatedRecallResult(req, base) {
		t.Fatal("valid peer result rejected")
	}
	cases := map[string]func(*MemoryResult){
		"hash collision":    func(r *MemoryResult) { r.Content = "different content" },
		"negative class":    func(r *MemoryResult) { r.Classification = -1 },
		"excess confidence": func(r *MemoryResult) { r.ConfidenceScore = 2 },
		"wrong domain":      func(r *MemoryResult) { r.DomainTag = "finance-secret" },
		"non-committed":     func(r *MemoryResult) { r.Status = "challenged" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			copy := *base
			mutate(&copy)
			if validFederatedRecallResult(req, &copy) {
				t.Fatalf("invalid peer result accepted: %+v", copy)
			}
		})
	}
}

func TestFanOutTargetCapConstantIsFinite(t *testing.T) {
	if maxFanOutTargets <= maxFanOutConcurrency || maxFanOutTargets > 256 {
		t.Fatalf("unsafe fan-out target cap: %d", maxFanOutTargets)
	}
}
