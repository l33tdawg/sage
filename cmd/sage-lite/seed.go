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
		fmt.Println(`Usage: sage-lite seed <file> [--domain <tag>]

Seed memories from a file into your SAGE brain.

File formats:
  .txt   Each paragraph (separated by blank lines) → one memory
  .json  Array of {"content", "domain", "type", "confidence"} objects
  .md    Each section (## heading + content) → one memory

Options:
  --domain <tag>   Default domain tag (default: "general")

Examples:
  sage-lite seed notes.txt --domain project
  sage-lite seed memories.json
  sage-lite seed chat-export.md --domain conversations`)
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
		baseURL = fmt.Sprintf("http://localhost%s", cfg.RESTAddr)
	}

	// Load agent key
	keyData, err := os.ReadFile(cfg.AgentKey)
	if err != nil {
		return fmt.Errorf("read agent key (run 'sage-lite mcp' once to generate): %w", err)
	}
	priv := ed25519.NewKeyFromSeed(keyData)
	pub, _ := priv.Public().(ed25519.PublicKey) //nolint:errcheck
	agentID := hex.EncodeToString(pub)

	// Read file
	data, err := os.ReadFile(filePath)
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

		// Get embedding
		embedding, err := getEmbedding(baseURL, mem.Content, agentID, priv)
		if err != nil {
			fmt.Printf("[%d/%d] FAIL (embed): %v\n", i+1, len(memories), err)
			continue
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
			fmt.Printf("[%d/%d] FAIL: %v\n", i+1, len(memories), err)
			continue
		}

		success++
		preview := mem.Content
		if len(preview) > 70 {
			preview = preview[:70] + "..."
		}
		fmt.Printf("[%d/%d] OK: %s\n", i+1, len(memories), preview)
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
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func doSignedHTTP(method, url string, body []byte, agentID string, priv ed25519.PrivateKey) (*http.Response, error) {
	ts := time.Now().Unix()
	h := sha256.Sum256(body)
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(ts)) //nolint:gosec // ts is always positive (Unix timestamp)
	msg := append(h[:], tsBuf[:]...)
	sig := ed25519.Sign(priv, msg)

	req, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-Signature", hex.EncodeToString(sig))

	return http.DefaultClient.Do(req)
}
