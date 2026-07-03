package main

// `sage-gui pair <token>` — the guest side of the LAN node-join ceremony
// (Phase 5b-3), for headless boxes or power users. It connects to the host's
// temporary pairing listener, proves possession of the secret carried in the
// token, shows the 6-digit code for the operator to compare, waits for the
// operator's approval, decrypts the bundle, and re-homes this node onto the
// host's chain as a non-validator peer.
//
// Usage: sage-gui pair <pairing-token> [--name NAME]

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cometbft/cometbft/p2p"

	"github.com/l33tdawg/sage/internal/pairing"
)

func runPair(args []string) error {
	token := ""
	force := false
	name, _ := os.Hostname()
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name":
			if i+1 < len(args) {
				name = args[i+1]
				i++
			}
		case "--force":
			force = true
		default:
			if token == "" {
				token = args[i]
			}
		}
	}
	if token == "" {
		return fmt.Errorf("usage: sage-gui pair <pairing-token> [--name NAME] [--force]")
	}

	tok, secret, err := pairing.DecodeToken(token)
	if err != nil {
		return fmt.Errorf("invalid pairing token: %w", err)
	}

	// Ensure this node has its own CometBFT identity so we can present a stable
	// node id (and so applyNodeJoinBundle has keys to keep).
	home := SageHome()
	cometHome := filepath.Join(home, "data", "cometbft")
	if initErr := initCometBFTConfig(cometHome); initErr != nil {
		return fmt.Errorf("init CometBFT: %w", initErr)
	}
	nodeKey, err := p2p.LoadNodeKey(filepath.Join(cometHome, "config", "node_key.json"))
	if err != nil {
		return fmt.Errorf("load node key: %w", err)
	}
	guestNodeID := string(nodeKey.ID())

	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return fmt.Errorf("generate nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)

	base := "http://" + tok.Addr
	client := &http.Client{Timeout: 10 * time.Second}

	// Step 1: hello.
	helloBody, _ := json.Marshal(map[string]string{
		"session_id":    tok.SessionID,
		"guest_node_id": guestNodeID,
		"guest_name":    name,
		"guest_nonce":   nonce,
		"proof":         pairing.ProofHello(secret, tok.SessionID, nonce, guestNodeID),
	})
	var hello struct {
		SAS   string `json:"sas"`
		Error string `json:"error"`
	}
	if code, err := pairPost(client, base+"/pair/hello", helloBody, &hello); err != nil {
		return fmt.Errorf("contact host: %w", err)
	} else if code != http.StatusOK {
		return fmt.Errorf("host rejected pairing: %s", firstNonEmpty(hello.Error, fmt.Sprintf("HTTP %d", code)))
	}

	// Cross-check: our locally-derived SAS must equal what the host returned.
	// A mismatch means something tampered with the exchange — abort.
	localSAS := pairing.SAS(secret, tok.SessionID, nonce)
	if hello.SAS != localSAS {
		return fmt.Errorf("security check failed: the host's code (%s) does not match ours (%s) — aborting", hello.SAS, localSAS)
	}

	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────┐")
	fmt.Printf("  │   Confirm this code on the HOST:  %s     │\n", localSAS)
	fmt.Println("  └─────────────────────────────────────────────┘")
	fmt.Println()
	fmt.Println("Waiting for the host operator to approve...")

	// Step 2: poll for the bundle until the operator approves (or we time out).
	bundleProof := pairing.ProofBundle(secret, tok.SessionID)
	bundleReq, _ := json.Marshal(map[string]string{"session_id": tok.SessionID, "proof": bundleProof})
	deadline := time.Now().Add(5 * time.Minute)
	var enc struct {
		Nonce      string `json:"nonce"`
		Ciphertext string `json:"ciphertext"`
		State      string `json:"state"`
		Error      string `json:"error"`
	}
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for host approval")
		}
		enc = struct {
			Nonce      string `json:"nonce"`
			Ciphertext string `json:"ciphertext"`
			State      string `json:"state"`
			Error      string `json:"error"`
		}{}
		code, err := pairPost(client, base+"/pair/bundle", bundleReq, &enc)
		if err != nil {
			return fmt.Errorf("fetch bundle: %w", err)
		}
		if code == http.StatusOK {
			break
		}
		// 202 = approved-not-yet; 429 = rate limited (shouldn't happen for a
		// valid proof, but back off rather than abort). Both are retryable.
		if code == http.StatusAccepted || code == http.StatusTooManyRequests {
			time.Sleep(2 * time.Second)
			continue
		}
		return fmt.Errorf("host declined bundle: %s", firstNonEmpty(enc.Error, fmt.Sprintf("HTTP %d", code)))
	}

	// Step 3: decrypt + apply.
	bundleJSON, err := pairing.DecryptBundle(secret, enc.Nonce, enc.Ciphertext)
	if err != nil {
		return fmt.Errorf("decrypt bundle: %w", err)
	}
	bundle, err := parseNodeJoinBundleJSON(bundleJSON)
	if err != nil {
		return err
	}
	if err := applyNodeJoinBundle(bundle, force); err != nil {
		return fmt.Errorf("apply join bundle: %w", err)
	}

	fmt.Println()
	fmt.Printf("Joined the network (chain %s).\n", bundle.ChainID)
	fmt.Println("This node is now a non-validator peer sharing the host's memory.")
	fmt.Println("Start it with:  sage-gui serve")
	return nil
}

// pairPost POSTs JSON and decodes the response into out. Returns the status code.
func pairPost(client *http.Client, url string, body []byte, out any) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode, nil
}
