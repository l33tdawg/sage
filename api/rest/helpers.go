package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
)

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeProblem writes an RFC 7807 Problem Details JSON response with the
// default status-derived problem type.
func writeProblem(w http.ResponseWriter, status int, title, detail string) {
	writeProblemTyped(w, status, fmt.Sprintf("https://sage.dev/errors/%d", status), title, detail)
}

// writeProblemTyped is writeProblem with an explicit problem type URI, for
// responses that must stay machine-distinguishable from other errors sharing
// the same status code (e.g. the mempool-full 429 vs the rate-limiter 429).
func writeProblemTyped(w http.ResponseWriter, status int, problemType, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"type":   problemType,
		"title":  title,
		"status": status,
		"detail": detail,
	})
}

// maxBodySize is the maximum allowed request body size (1 MB).
const maxBodySize = 1 << 20

// decodeJSON reads and unmarshals the request body as JSON.
func decodeJSON(r *http.Request, v interface{}) error {
	r.Body = http.MaxBytesReader(nil, r.Body, maxBodySize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	defer r.Body.Close()
	if len(body) == 0 {
		return fmt.Errorf("empty request body")
	}
	// Replace body so downstream handlers can re-read it.
	r.Body = io.NopCloser(bytes.NewReader(body))
	return json.Unmarshal(body, v)
}

// Bounds on what an ignored-field report echoes back, so a hostile body cannot
// turn the report into a large response or log line.
const (
	maxIgnoredFieldsReported = 16
	maxIgnoredFieldNameLen   = 64
)

// jsonFieldNames returns the top-level JSON names a struct type decodes.
func jsonFieldNames(t reflect.Type) map[string]struct{} {
	names := make(map[string]struct{}, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = f.Name
		}
		names[name] = struct{}{}
	}
	return names
}

// ignoredJSONFields lists the top-level keys in body that json.Unmarshal into
// a value with the known names would silently drop. Go matches keys to fields
// case-insensitively, so a key is ignored only when no known name matches it
// under strings.EqualFold. The result is sorted and bounded; a body that is not
// a JSON object reports nothing.
func ignoredJSONFields(body []byte, known map[string]struct{}) []string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil
	}
	var ignored []string
	for key := range fields {
		if _, ok := known[key]; ok {
			continue
		}
		matched := false
		for name := range known {
			if strings.EqualFold(key, name) {
				matched = true
				break
			}
		}
		if !matched {
			ignored = append(ignored, key)
		}
	}
	sort.Strings(ignored)
	if len(ignored) > maxIgnoredFieldsReported {
		ignored = ignored[:maxIgnoredFieldsReported]
	}
	for i, key := range ignored {
		if len(key) > maxIgnoredFieldNameLen {
			ignored[i] = key[:maxIgnoredFieldNameLen]
		}
	}
	return ignored
}

func jsonFieldPresent(r *http.Request, field string) (bool, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return false, fmt.Errorf("read body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return false, err
	}
	_, ok := fields[field]
	return ok, nil
}
