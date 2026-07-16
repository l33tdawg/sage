package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
)

// configureValidatorStateSyncP2P applies the fail-closed CometBFT profile
// required before SAGE may advertise or receive a network state-sync snapshot.
// Persistent peers are the fixed validator/provider mesh. authorizedPeerIDs
// drives the matching capacity/privacy sets and, critically, the authenticated
// ABCI node-ID filter enforced by BootStateSyncRuntime.
func configureValidatorStateSyncP2P(cometCfg *config.Config, persistentPeers, authorizedPeerIDs []string) error {
	if cometCfg == nil || cometCfg.P2P == nil {
		return errors.New("validator state-sync P2P config is required")
	}
	cfg := cometCfg.P2P
	ids, err := canonicalStateSyncPeerIDs(authorizedPeerIDs)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return errors.New("validator state-sync P2P profile requires an authorized peer")
	}
	authorized := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		authorized[id] = struct{}{}
	}

	peers := make([]string, 0, len(persistentPeers))
	seenPeers := make(map[string]struct{}, len(persistentPeers))
	for _, raw := range persistentPeers {
		peer := strings.TrimSpace(raw)
		if peer == "" {
			return errors.New("validator state-sync persistent peer is empty")
		}
		address, parseErr := p2p.NewNetAddressString(peer)
		if parseErr != nil {
			return fmt.Errorf("invalid validator state-sync persistent peer %q: %w", peer, parseErr)
		}
		id := string(address.ID)
		if _, ok := authorized[id]; !ok {
			return fmt.Errorf("persistent peer %s is absent from the validator state-sync allowlist", id)
		}
		if _, duplicate := seenPeers[id]; duplicate {
			return fmt.Errorf("duplicate validator state-sync persistent peer ID %s", id)
		}
		seenPeers[id] = struct{}{}
		peers = append(peers, peer)
	}
	sort.Strings(peers)

	allowlist := strings.Join(ids, ",")
	cfg.Seeds = ""
	cfg.PersistentPeers = strings.Join(peers, ",")
	cfg.PexReactor = false
	cfg.SeedMode = false
	cfg.MaxNumInboundPeers = 0
	cfg.MaxNumOutboundPeers = 0
	cfg.UnconditionalPeerIDs = allowlist
	cfg.PrivatePeerIDs = allowlist
	// CometBFT's unconditional/private peer sets are not authorization filters:
	// they affect capacity and address gossip only. The authenticated node-ID
	// callback is the enforcement boundary for this exact allowlist.
	cometCfg.FilterPeers = true
	return validateValidatorStateSyncP2P(cometCfg, ids)
}

// validateValidatorStateSyncP2P is also used as the endpoint arming gate. It
// checks the effective CometBFT config, not merely the operator's input.
func validateValidatorStateSyncP2P(cometCfg *config.Config, authorizedPeerIDs []string) error {
	if cometCfg == nil || cometCfg.P2P == nil {
		return errors.New("validator state-sync P2P config is required")
	}
	if !cometCfg.FilterPeers {
		return errors.New("validator state-sync P2P profile requires authenticated peer filtering")
	}
	cfg := cometCfg.P2P
	want, err := canonicalStateSyncPeerIDs(authorizedPeerIDs)
	if err != nil {
		return err
	}
	if len(want) == 0 {
		return errors.New("validator state-sync P2P profile requires an authorized peer")
	}
	if strings.TrimSpace(cfg.Seeds) != "" || cfg.PexReactor || cfg.SeedMode {
		return errors.New("validator state-sync P2P profile forbids seeds, PEX, and seed mode")
	}
	if cfg.MaxNumInboundPeers != 0 || cfg.MaxNumOutboundPeers != 0 {
		return errors.New("validator state-sync P2P profile requires zero ordinary inbound and outbound peer capacity")
	}
	unconditional, err := canonicalStateSyncPeerIDs(splitPeerIDs(cfg.UnconditionalPeerIDs))
	if err != nil {
		return fmt.Errorf("invalid unconditional state-sync peer IDs: %w", err)
	}
	private, err := canonicalStateSyncPeerIDs(splitPeerIDs(cfg.PrivatePeerIDs))
	if err != nil {
		return fmt.Errorf("invalid private state-sync peer IDs: %w", err)
	}
	if !equalStrings(want, unconditional) || !equalStrings(want, private) {
		return errors.New("validator state-sync P2P unconditional/private peer IDs must exactly match the authorization allowlist")
	}

	authorized := make(map[string]struct{}, len(want))
	for _, id := range want {
		authorized[id] = struct{}{}
	}
	seen := make(map[string]struct{})
	for _, raw := range splitPeerIDs(cfg.PersistentPeers) {
		address, parseErr := p2p.NewNetAddressString(raw)
		if parseErr != nil {
			return fmt.Errorf("invalid validator state-sync persistent peer %q: %w", raw, parseErr)
		}
		id := string(address.ID)
		if _, ok := authorized[id]; !ok {
			return fmt.Errorf("persistent peer %s is absent from the validator state-sync allowlist", id)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("duplicate validator state-sync persistent peer ID %s", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func canonicalStateSyncPeerIDs(raw []string) ([]string, error) {
	ids := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, value := range raw {
		id := strings.TrimSpace(value)
		if id == "" {
			return nil, errors.New("validator state-sync peer ID is empty")
		}
		decoded, err := hex.DecodeString(id)
		if err != nil || len(decoded) != p2p.IDByteLength || id != strings.ToLower(id) {
			return nil, fmt.Errorf("invalid CometBFT peer ID %q", id)
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("duplicate validator state-sync peer ID %s", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func splitPeerIDs(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
