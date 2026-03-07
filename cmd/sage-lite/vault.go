package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// VaultHeader is the metadata at the top of a .vault file.
type VaultHeader struct {
	Version   int    `json:"version"`
	CreatedAt string `json:"created_at"`
	AgentID   string `json:"agent_id"`
	Encrypted bool   `json:"encrypted"`
	Memories  int    `json:"memories"`
}

// VaultMemory is a single memory in the vault export.
type VaultMemory struct {
	MemoryID        string  `json:"memory_id"`
	Content         string  `json:"content"`
	MemoryType      string  `json:"memory_type"`
	DomainTag       string  `json:"domain_tag"`
	ConfidenceScore float64 `json:"confidence_score"`
	Status          string  `json:"status"`
	CreatedAt       string  `json:"created_at"`
}

// VaultFile is the complete vault export structure.
type VaultFile struct {
	Header   VaultHeader   `json:"header"`
	Memories []VaultMemory `json:"memories"`
}

func runExport() error {
	outputPath := "memories.vault"
	encrypt := false

	// Parse flags
	for i := 2; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--encrypt", "-e":
			encrypt = true
		default:
			if os.Args[i][0] != '-' {
				outputPath = os.Args[i]
			}
		}
	}

	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = "http://localhost" + cfg.RESTAddr
	}

	fmt.Println("Exporting memories from SAGE...")

	// Fetch all memories via dashboard API (no auth needed for local dashboard).
	allMemories := make([]VaultMemory, 0)
	offset := 0
	limit := 200

	for {
		listURL := fmt.Sprintf("%s/v1/dashboard/memory/list?limit=%d&offset=%d&sort=oldest", baseURL, limit, offset)
		listReq, _ := http.NewRequestWithContext(context.Background(), "GET", listURL, nil)
		resp, doErr := http.DefaultClient.Do(listReq)
		if doErr != nil {
			return fmt.Errorf("fetch memories (is sage-lite serve running?): %w", doErr)
		}

		var listResp struct {
			Memories []VaultMemory `json:"memories"`
			Total    int           `json:"total"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&listResp); decodeErr != nil {
			resp.Body.Close()
			return fmt.Errorf("decode response: %w", decodeErr)
		}
		resp.Body.Close()

		allMemories = append(allMemories, listResp.Memories...)

		if len(allMemories) >= listResp.Total || len(listResp.Memories) == 0 {
			break
		}
		offset += limit
	}

	if len(allMemories) == 0 {
		fmt.Println("No memories to export.")
		return nil
	}

	// Read agent ID for header.
	agentID := "unknown"
	keyData, err := os.ReadFile(cfg.AgentKey)
	if err == nil && len(keyData) == ed25519.SeedSize {
		privKey := ed25519.NewKeyFromSeed(keyData)
		pubKey, _ := privKey.Public().(ed25519.PublicKey) //nolint:errcheck
		agentID = fmt.Sprintf("%x", pubKey[:8])
	}

	vault := VaultFile{
		Header: VaultHeader{
			Version:   1,
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
			AgentID:   agentID,
			Encrypted: encrypt,
			Memories:  len(allMemories),
		},
		Memories: allMemories,
	}

	vaultJSON, err := json.MarshalIndent(vault, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal vault: %w", err)
	}

	var outputData []byte
	if encrypt {
		outputData, err = encryptVault(vaultJSON, cfg.AgentKey)
		if err != nil {
			return fmt.Errorf("encrypt vault: %w", err)
		}
		fmt.Println("Encrypted with your agent key.")
	} else {
		outputData = vaultJSON
	}

	if err := os.WriteFile(outputPath, outputData, 0600); err != nil {
		return fmt.Errorf("write vault file: %w", err)
	}

	fmt.Printf("Exported %d memories to %s\n", len(allMemories), outputPath)
	if !encrypt {
		fmt.Println("Tip: use --encrypt to protect your memories with your agent key.")
	}
	return nil
}

func runImport() error {
	if len(os.Args) < 3 {
		return fmt.Errorf("usage: sage-lite import <file.vault> [--key <agent.key>]")
	}

	inputPath := os.Args[2]
	keyPath := ""

	for i := 3; i < len(os.Args); i++ {
		if (os.Args[i] == "--key" || os.Args[i] == "-k") && i+1 < len(os.Args) {
			keyPath = os.Args[i+1]
			i++
		}
	}

	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if keyPath == "" {
		keyPath = cfg.AgentKey
	}

	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = "http://localhost" + cfg.RESTAddr
	}

	// Read vault file.
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("read vault file: %w", err)
	}

	// Try to parse as JSON first (unencrypted).
	var vault VaultFile
	if unmarshalErr := json.Unmarshal(data, &vault); unmarshalErr != nil {
		// Might be encrypted — try decrypting.
		fmt.Println("Vault appears encrypted, decrypting with agent key...")
		decrypted, decErr := decryptVault(data, keyPath)
		if decErr != nil {
			return fmt.Errorf("decrypt vault (wrong key?): %w", decErr)
		}
		if parseErr := json.Unmarshal(decrypted, &vault); parseErr != nil {
			return fmt.Errorf("parse decrypted vault: %w", parseErr)
		}
	}

	fmt.Printf("Vault: %d memories, created %s\n", vault.Header.Memories, vault.Header.CreatedAt)

	// Load agent key for signing imports.
	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("read agent key: %w", err)
	}

	var privKey ed25519.PrivateKey
	if len(keyData) == ed25519.SeedSize {
		privKey = ed25519.NewKeyFromSeed(keyData)
	} else if len(keyData) == ed25519.PrivateKeySize {
		privKey = ed25519.PrivateKey(keyData)
	} else {
		return fmt.Errorf("invalid agent key size (expected %d or %d bytes, got %d)", ed25519.SeedSize, ed25519.PrivateKeySize, len(keyData))
	}

	// Import each memory via the REST API.
	imported := 0
	skipped := 0
	for i, mem := range vault.Memories {
		fmt.Printf("\r  Importing %d/%d...", i+1, len(vault.Memories))

		// Get embedding.
		embedReq, _ := json.Marshal(map[string]string{"text": mem.Content})
		embedResp, err := doSignedRequest(baseURL, privKey, "POST", "/v1/embed", embedReq)
		if err != nil {
			skipped++
			continue
		}
		var embedResult struct {
			Embedding []float32 `json:"embedding"`
		}
		_ = json.Unmarshal(embedResp, &embedResult)

		// Submit memory.
		submitReq, _ := json.Marshal(map[string]any{
			"content":          mem.Content,
			"memory_type":      mem.MemoryType,
			"domain_tag":       mem.DomainTag,
			"confidence_score": mem.ConfidenceScore,
			"embedding":        embedResult.Embedding,
		})
		_, err = doSignedRequest(baseURL, privKey, "POST", "/v1/memory/submit", submitReq)
		if err != nil {
			skipped++
			continue
		}
		imported++
	}

	fmt.Printf("\rImported %d memories, skipped %d.          \n", imported, skipped)
	return nil
}

// doSignedRequest makes an Ed25519-signed HTTP request (shared with seed.go pattern).
// Signs method + path + body + timestamp to prevent cross-endpoint replay.
func doSignedRequest(baseURL string, key ed25519.PrivateKey, method, path string, body []byte) ([]byte, error) {
	timestamp := time.Now().Unix()
	// Build canonical request: "METHOD /path\n<body>"
	canonical := []byte(method + " " + path + "\n")
	canonical = append(canonical, body...)
	hash := sha256.Sum256(canonical)
	msg := make([]byte, 40)
	copy(msg[:32], hash[:])
	msg[32] = byte(timestamp >> 56)
	msg[33] = byte(timestamp >> 48)
	msg[34] = byte(timestamp >> 40)
	msg[35] = byte(timestamp >> 32)
	msg[36] = byte(timestamp >> 24)
	msg[37] = byte(timestamp >> 16)
	msg[38] = byte(timestamp >> 8)
	msg[39] = byte(timestamp)

	sig := ed25519.Sign(key, msg)
	pub, _ := key.Public().(ed25519.PublicKey) //nolint:errcheck

	req, err := http.NewRequestWithContext(context.Background(), method, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", fmt.Sprintf("%x", pub))
	req.Header.Set("X-Signature", fmt.Sprintf("%x", sig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", timestamp))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

// encryptVault encrypts vault JSON using AES-256-GCM derived from the agent's Ed25519 key.
func encryptVault(plaintext []byte, keyPath string) ([]byte, error) {
	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}

	// Derive AES key from agent key via SHA-256.
	aesKey := sha256.Sum256(keyData)

	block, err := aes.NewCipher(aesKey[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Prepend magic bytes so we can detect encrypted vaults.
	magic := []byte("SAGEVAULT1")
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return append(magic, ciphertext...), nil
}

// decryptVault decrypts an encrypted vault file.
func decryptVault(data []byte, keyPath string) ([]byte, error) {
	magic := []byte("SAGEVAULT1")
	if len(data) < len(magic) {
		return nil, fmt.Errorf("file too small")
	}

	// Strip magic bytes if present.
	if string(data[:len(magic)]) == string(magic) {
		data = data[len(magic):]
	}

	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}

	aesKey := sha256.Sum256(keyData)

	block, err := aes.NewCipher(aesKey[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

func runBackup() error {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	home := SageHome()
	backupDir := filepath.Join(home, "backups")
	if mkdirErr := os.MkdirAll(backupDir, 0700); mkdirErr != nil {
		return fmt.Errorf("create backup dir: %w", mkdirErr)
	}

	// Find the SQLite database.
	dbPath := filepath.Join(cfg.DataDir, "sage.db")
	if _, statErr := os.Stat(dbPath); statErr != nil {
		return fmt.Errorf("database not found at %s (is SAGE initialized?)", dbPath)
	}

	// Copy database to backup with timestamp.
	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05")
	backupPath := filepath.Join(backupDir, fmt.Sprintf("sage-%s.db", timestamp))

	src, readErr := os.ReadFile(dbPath)
	if readErr != nil {
		return fmt.Errorf("read database: %w", readErr)
	}

	if writeErr := os.WriteFile(backupPath, src, 0600); writeErr != nil {
		return fmt.Errorf("write backup: %w", writeErr)
	}

	fmt.Printf("Backup saved: %s (%d bytes)\n", backupPath, len(src))

	// Rotate old backups: keep 24 hourly + 7 daily.
	rotateBackups(backupDir)

	return nil
}

// rotateBackups keeps the most recent 30 backups and removes older ones.
func rotateBackups(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	// Collect .db files sorted by name (timestamps sort naturally).
	var backups []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".db" {
			backups = append(backups, filepath.Join(dir, e.Name()))
		}
	}

	// Keep the 30 most recent (24 hourly + some daily buffer).
	maxKeep := 30
	if len(backups) > maxKeep {
		toRemove := backups[:len(backups)-maxKeep]
		for _, path := range toRemove {
			os.Remove(path)
		}
		fmt.Printf("Rotated %d old backups.\n", len(toRemove))
	}
}
