package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// runSeed loads memories from a file and submits them to the running SAGE node.
// Supports two formats:
//   - Plain text: each paragraph (separated by blank lines) becomes a memory
//   - JSON: array of objects with "content", "domain", "type", "confidence" fields
func runSeed() error {
	if len(os.Args) < 3 {
		fmt.Println(`Usage: sage-gui seed <file> [--domain <tag>]

Seed memories from a file into your SAGE brain.

File formats:
  .txt   Each paragraph (separated by blank lines) → one memory
  .json  Array of {"content", "domain", "type", "confidence"} objects
  .md    Each section (## heading + content) → one memory

Options:
  --domain <tag>   Default domain tag (default: "general")

Examples:
  sage-gui seed notes.txt --domain project
  sage-gui seed memories.json
  sage-gui seed chat-export.md --domain conversations`)
		return nil
	}

	filePath := os.Args[2]
	domain := "general"

	// Parse optional --domain flag
	for i := 3; i < len(os.Args)-1; i++ {
		if os.Args[i] == "--domain" {
			domain = os.Args[i+1]
		}
	}

	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = restBaseURL(cfg.RESTAddr)
	}

	// Load agent key
	keyData, err := os.ReadFile(cfg.AgentKey)
	if err != nil {
		return fmt.Errorf("read agent key (run 'sage-gui mcp' once to generate): %w", err)
	}
	priv := ed25519.NewKeyFromSeed(keyData)
	pub, _ := priv.Public().(ed25519.PublicKey) //nolint:errcheck
	agentID := hex.EncodeToString(pub)

	// Read file
	data, err := os.ReadFile(filePath) //nolint:gosec // filePath is from CLI args
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	var memories []seedMemory

	switch {
	case strings.HasSuffix(filePath, ".json"):
		if err := json.Unmarshal(data, &memories); err != nil {
			return fmt.Errorf("parse JSON: %w", err)
		}
	case strings.HasSuffix(filePath, ".md"):
		memories = parseMarkdown(string(data), domain)
	default:
		memories = parseParagraphs(string(data), domain)
	}

	if len(memories) == 0 {
		return fmt.Errorf("no memories found in %s", filePath)
	}

	fmt.Printf("Seeding %d memories from %s (domain: %s)\n\n", len(memories), filePath, domain)

	const maxRetries = 3

	success := 0
	for i, mem := range memories {
		if mem.Domain == "" {
			mem.Domain = domain
		}
		if mem.Type == "" {
			mem.Type = "observation"
		}
		if mem.Confidence == 0 {
			mem.Confidence = 0.85
		}

		var lastErr error
		for attempt := range maxRetries {
			lastErr = nil

			// Get embedding
			embedding, err := getEmbedding(baseURL, mem.Content, agentID, priv)
			if err != nil {
				lastErr = fmt.Errorf("embed: %w", err)
				if backoffOnRetry(attempt, maxRetries, err) {
					continue
				}
				break
			}

			// Submit memory
			body, _ := json.Marshal(map[string]any{
				"content":          mem.Content,
				"memory_type":      mem.Type,
				"domain_tag":       mem.Domain,
				"confidence_score": mem.Confidence,
				"embedding":        embedding,
			})

			if err := submitSigned(baseURL+"/v1/memory/submit", body, agentID, priv); err != nil {
				lastErr = err
				if backoffOnRetry(attempt, maxRetries, err) {
					continue
				}
				break
			}

			break
		}

		if lastErr != nil {
			fmt.Printf("[%d/%d] FAIL: %v\n", i+1, len(memories), lastErr)
			continue
		}

		success++
		preview := mem.Content
		if len(preview) > 70 {
			preview = preview[:70] + "..."
		}
		fmt.Printf("[%d/%d] OK: %s\n", i+1, len(memories), preview)

		// Small delay between seeds to avoid overwhelming the consensus pipeline.
		if i < len(memories)-1 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	fmt.Printf("\nSeeded %d/%d memories successfully.\n", success, len(memories))
	return nil
}

type seedMemory struct {
	Content    string  `json:"content"`
	Domain     string  `json:"domain,omitempty"`
	Type       string  `json:"type,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
}

func parseParagraphs(text, domain string) []seedMemory {
	paragraphs := strings.Split(text, "\n\n")
	memories := make([]seedMemory, 0, len(paragraphs))
	for _, p := range paragraphs {
		p = strings.TrimSpace(p)
		if len(p) < 20 { // Skip very short paragraphs
			continue
		}
		memories = append(memories, seedMemory{
			Content:    p,
			Domain:     domain,
			Type:       "observation",
			Confidence: 0.85,
		})
	}
	return memories
}

func parseMarkdown(text, domain string) []seedMemory {
	var memories []seedMemory
	scanner := bufio.NewScanner(strings.NewReader(text))
	var current strings.Builder
	var currentHeading string

	flush := func() {
		content := strings.TrimSpace(current.String())
		if len(content) < 20 {
			current.Reset()
			return
		}
		if currentHeading != "" {
			content = currentHeading + ": " + content
		}
		memories = append(memories, seedMemory{
			Content:    content,
			Domain:     domain,
			Type:       "observation",
			Confidence: 0.85,
		})
		current.Reset()
	}

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "## ") {
			flush()
			currentHeading = strings.TrimPrefix(line, "## ")
		} else if strings.HasPrefix(line, "# ") {
			flush()
			currentHeading = strings.TrimPrefix(line, "# ")
		} else {
			current.WriteString(line)
			current.WriteString("\n")
		}
	}
	flush()
	return memories
}

func getEmbedding(baseURL, text, agentID string, priv ed25519.PrivateKey) ([]float32, error) {
	body, _ := json.Marshal(map[string]string{"text": text})
	resp, err := doSignedHTTP("POST", baseURL+"/v1/embed", body, agentID, priv)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Embedding, nil
}

func submitSigned(url string, body []byte, agentID string, priv ed25519.PrivateKey) error {
	resp, err := doSignedHTTP("POST", url, body, agentID, priv)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MB max
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// backoffOnRetry returns true if the caller should retry after sleeping.
// It implements exponential backoff: 2s, 4s, 8s, etc.
// Returns false on the last attempt so the caller can stop.
func backoffOnRetry(attempt, maxRetries int, err error) bool {
	if attempt >= maxRetries-1 {
		return false
	}
	errStr := err.Error()
	// Longer wait for rate limits
	if strings.Contains(errStr, "429") {
		fmt.Printf("  rate limited, waiting 60s before retry...\n")
		time.Sleep(60 * time.Second)
		return true
	}
	// Exponential backoff for server errors (500, SQLITE_BUSY, etc.)
	if strings.Contains(errStr, "500") || strings.Contains(errStr, "BUSY") {
		wait := time.Duration(2<<uint(attempt)) * time.Second //nolint:gosec // attempt is bounded by maxRetries (3)
		fmt.Printf("  server busy, retrying in %v...\n", wait)
		time.Sleep(wait)
		return true
	}
	return false
}

func doSignedHTTP(method, url string, body []byte, agentID string, priv ed25519.PrivateKey) (*http.Response, error) {
	ts := time.Now().Unix()
	// Extract path from URL for canonical signing.
	path := url
	if idx := strings.Index(url, "://"); idx >= 0 {
		rest := url[idx+3:]
		if pathIdx := strings.Index(rest, "/"); pathIdx >= 0 {
			path = rest[pathIdx:]
		}
	}
	// Build canonical request: "METHOD /path\n<body>"
	canonical := []byte(method + " " + path + "\n")
	canonical = append(canonical, body...)
	h := sha256.Sum256(canonical)
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(ts)) //nolint:gosec // ts is always positive (Unix timestamp)
	msg := append(h[:], tsBuf[:]...)
	sig := ed25519.Sign(priv, msg)

	req, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body)) //nolint:gosec // url is from config baseURL
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-Signature", hex.EncodeToString(sig))

	client := tlsAwareClient(url)
	return client.Do(req) //nolint:gosec // internal API call
}
