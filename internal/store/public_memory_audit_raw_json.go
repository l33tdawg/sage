package store

import (
	"encoding/base64"
	"encoding/json"
)

// AuditV2 is diagnostic transport, not a consensus/signing payload. Selection
// is made from the audit's qualified held snapshot, never from UTF-8 content.
// Legacy audits keep their original JSON representation byte-for-byte.
func (audit PublicMemoryAudit) MarshalJSON() ([]byte, error) {
	type legacyAudit PublicMemoryAudit
	if !audit.rawIdentityRules {
		return json.Marshal(legacyAudit(audit))
	}
	type rawDefect struct {
		MemoryIDBytes   string   `json:"memory_id_bytes"`
		Status          string   `json:"status"`
		Reasons         []string `json:"reasons"`
		BornUnderAppV25 bool     `json:"born_under_app_v25"`
		Quarantined     bool     `json:"quarantined"`
	}
	type rawAudit struct {
		Version       int         `json:"version"`
		IdentityRules string      `json:"identity_rules"`
		IDEncoding    string      `json:"id_encoding"`
		Scanned       int         `json:"scanned"`
		Public        int         `json:"public"`
		Quarantined   int         `json:"quarantined"`
		Blocking      int         `json:"blocking"`
		Defects       []rawDefect `json:"defects"`
		Truncated     bool        `json:"truncated"`
	}
	encoded := rawAudit{Version: 2, IdentityRules: "app-v30-raw", IDEncoding: "base64-std-padded", Scanned: audit.Scanned, Public: audit.Public, Quarantined: audit.Quarantined, Blocking: audit.Blocking, Truncated: audit.Truncated, Defects: make([]rawDefect, 0, len(audit.Defects))}
	for _, defect := range audit.Defects {
		encoded.Defects = append(encoded.Defects, rawDefect{MemoryIDBytes: base64.StdEncoding.EncodeToString([]byte(defect.MemoryID)), Status: defect.Status, Reasons: defect.Reasons, BornUnderAppV25: defect.BornUnderAppV25, Quarantined: defect.Quarantined})
	}
	return json.Marshal(encoded)
}
