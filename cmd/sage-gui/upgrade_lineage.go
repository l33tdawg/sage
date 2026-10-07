package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/tx"
)

type upgradeLineageRPCStatus struct {
	Schema             string                  `json:"schema"`
	CurrentAppVersion  uint64                  `json:"current_app_version"`
	PersistedHeight    int64                   `json:"persisted_height"`
	GovernanceDomain   string                  `json:"governance_domain"`
	ValidLineageDigest string                  `json:"valid_lineage_digest"`
	RepairAudit        any                     `json:"repair_audit,omitempty"`
	Rungs              []upgradeLineageRPCRung `json:"rungs"`
}

type upgradeLineageRPCRung struct {
	Version           uint64 `json:"version"`
	Name              string `json:"name"`
	Present           bool   `json:"present"`
	AppliedHeight     int64  `json:"applied_height,omitempty"`
	Valid             bool   `json:"valid"`
	Problem           string `json:"problem,omitempty"`
	Provenance        string `json:"provenance,omitempty"`
	SubsumedByVersion uint64 `json:"subsumed_by_version,omitempty"`
	Virtual           bool   `json:"virtual,omitempty"`
	TransitionTarget  bool   `json:"transition_target,omitempty"`
}

type lineageDoctorOutput struct {
	Status             upgradeLineageRPCStatus `json:"status"`
	Repairable         bool                    `json:"repairable"`
	ManualVoteRequired bool                    `json:"manual_vote_required"`
	ManifestDigest     string                  `json:"manifest_digest,omitempty"`
	Diagnostics        []string                `json:"diagnostics"`
	ReviewSteps        []string                `json:"review_steps,omitempty"`
	Manifest           json.RawMessage         `json:"manifest,omitempty"`
}

type lineageVerifyOutput struct {
	Valid           bool                                   `json:"valid"`
	HistoryVerified bool                                   `json:"history_verified"`
	EligibleForVote bool                                   `json:"eligible_for_manual_vote"`
	ManifestDigest  string                                 `json:"manifest_digest"`
	EvidenceSource  string                                 `json:"evidence_source,omitempty"`
	Claims          []sageabci.LegacyLineageEvidence       `json:"claims"`
	Transitions     []sageabci.LineageActivationTransition `json:"transitions,omitempty"`
	Diagnostics     []string                               `json:"diagnostics"`
}

type retainedLineageEvidence struct {
	Claims      []sageabci.LegacyLineageEvidence
	Transitions []sageabci.LineageActivationTransition
	Complete    bool `json:"-"`
}

type legacyAnchorTransition struct {
	FromVersion      uint64   `json:"from_version"`
	ToVersion        uint64   `json:"to_version"`
	AppliedHeight    int64    `json:"applied_height"`
	SubsumedVersions []uint64 `json:"subsumed_versions"`
}

type legacyAnchorDocument struct {
	Heights     map[string]int64         `json:"heights,omitempty"`
	Transitions []legacyAnchorTransition `json:"transitions,omitempty"`
}

func runUpgradeLineage(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sage-gui upgrade lineage status|doctor --json")
	}
	switch args[0] {
	case "status":
		return runUpgradeLineageStatus(args[1:])
	case "doctor":
		return runUpgradeLineageDoctor(args[1:])
	case "verify":
		return runUpgradeLineageVerify(args[1:])
	default:
		return fmt.Errorf("unknown upgrade lineage subcommand %q", args[0])
	}
}

func runUpgradeLineageVerify(args []string) error {
	fs := flag.NewFlagSet("upgrade lineage verify", flag.ContinueOnError)
	rpc := fs.String("rpc", defaultCometRPC(), "CometBFT RPC endpoint")
	manifestPath := fs.String("manifest", "", "canonical lineage repair manifest to verify")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	ackAnchor := fs.Bool("acknowledge-unverified-anchor", false, "explicitly accept that anchor claims cannot be checked against retained history")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return errors.New("--manifest is required")
	}
	raw, err := os.ReadFile(*manifestPath) //nolint:gosec // explicit operator path
	if err != nil {
		return err
	}
	canonical, err := sageabci.CanonicalizeLegacyLineageRepair(raw)
	if err != nil {
		return err
	}
	var manifest sageabci.LegacyLineageRepairManifest
	if err := json.Unmarshal([]byte(canonical), &manifest); err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(canonical))
	out := lineageVerifyOutput{ManifestDigest: hex.EncodeToString(digest[:]), Claims: manifest.Evidence, Transitions: manifest.Transitions}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	status, statusErr := readUpgradeLineageStatus(ctx, *rpc)
	chainID, chainErr := readCometChainID(ctx, *rpc)
	if statusErr != nil {
		out.Diagnostics = append(out.Diagnostics, "read local lineage status: "+statusErr.Error())
	}
	if chainErr != nil {
		out.Diagnostics = append(out.Diagnostics, "read local chain ID: "+chainErr.Error())
	}
	if statusErr == nil && chainErr == nil {
		if manifest.Schema != status.Schema || manifest.ChainID != chainID ||
			manifest.GovernanceDomain != status.GovernanceDomain || manifest.CurrentAppVersion != 21 ||
			status.CurrentAppVersion != 21 || manifest.PriorLineageDigest != status.ValidLineageDigest {
			out.Diagnostics = append(out.Diagnostics, "manifest chain, app version, governance domain, or prior-lineage digest does not match this validator")
		} else {
			missing := make(map[uint64]struct{})
			for _, rung := range status.Rungs {
				if rung.Present && !rung.Valid {
					out.Diagnostics = append(out.Diagnostics, rung.Name+" is present but invalid; create-only repair is forbidden")
				}
				if !rung.Present {
					missing[rung.Version] = struct{}{}
				}
			}
			if len(manifest.Evidence)+len(manifest.Transitions) == 0 {
				out.Diagnostics = append(out.Diagnostics, "manifest contains no missing-rung claims")
			} else {
				source := "comet-block-results"
				if len(manifest.Evidence) > 0 {
					source = manifest.Evidence[0].Source
				} else if len(manifest.Transitions) > 0 {
					source = manifest.Transitions[0].Source
				}
				out.EvidenceSource = source
				switch source {
				case "legacy-anchor":
					if !exactLineageCoverage(missing, manifest.Evidence, manifest.Transitions) || !validAnchorTrustBundle(status, manifest.Evidence, manifest.Transitions) {
						out.Diagnostics = append(out.Diagnostics, "legacy-anchor manifest does not exactly cover the current missing-rung set")
					} else {
						anchorBytes, anchorErr := hex.DecodeString(manifest.AnchorDigest)
						canonicalAnchor := anchorErr == nil && len(anchorBytes) == sha256.Size && hex.EncodeToString(anchorBytes) == manifest.AnchorDigest
						if !canonicalAnchor || !anchorDigestMatchesManifest(manifest) {
							out.Diagnostics = append(out.Diagnostics, "legacy-anchor manifest has no canonical anchor_digest")
						} else if manifest.AnchorAttestation != "operator-quorum-attested-unverified-history" || !*ackAnchor {
							out.Diagnostics = append(out.Diagnostics, "legacy-anchor claims are unverified history and require --acknowledge-unverified-anchor before an operator vote")
						} else {
							out.Valid, out.EligibleForVote = true, true
							out.Diagnostics = append(out.Diagnostics, "anchor heights are operator claims, not locally history-verified facts; the explicit vote attests these exact claims")
						}
					}
				case "comet-block-results":
					local, diagnostics := discoverRetainedLineageEvidence(ctx, *rpc, status, missing)
					out.Diagnostics = append(out.Diagnostics, diagnostics...)
					if !local.Complete {
						out.Diagnostics = append(out.Diagnostics, "this validator could not replay retained version history through the committed tip")
					} else if !exactLineageCoverage(missing, manifest.Evidence, manifest.Transitions) {
						out.Diagnostics = append(out.Diagnostics, "manifest retained-history evidence does not exactly cover the current missing-rung set")
					} else if !equalRetainedLineageEvidence(local, retainedLineageEvidence{Claims: manifest.Evidence, Transitions: manifest.Transitions}) {
						out.Diagnostics = append(out.Diagnostics, "manifest does not match this validator's independently reconstructed retained version-transition history")
					} else {
						out.HistoryVerified, out.Valid, out.EligibleForVote = true, true, true
					}
				default:
					out.Diagnostics = append(out.Diagnostics, "unsupported or mixed evidence source")
				}
			}
		}
	}
	if *jsonOut {
		if err := writeJSONStdout(out); err != nil {
			return err
		}
	} else {
		for _, diagnostic := range out.Diagnostics {
			fmt.Println(diagnostic)
		}
	}
	if !out.EligibleForVote {
		return errors.New("lineage manifest is not eligible for this validator's manual vote")
	}
	return nil
}

func runUpgradeLineageStatus(args []string) error {
	fs := flag.NewFlagSet("upgrade lineage status", flag.ContinueOnError)
	rpc := fs.String("rpc", defaultCometRPC(), "CometBFT RPC endpoint")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	status, err := readUpgradeLineageStatus(ctx, *rpc)
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSONStdout(status)
	}
	fmt.Printf("Upgrade lineage: app-v%d at height %d\n", status.CurrentAppVersion, status.PersistedHeight)
	for _, rung := range status.Rungs {
		state := "missing"
		if rung.Valid && rung.Virtual {
			state = fmt.Sprintf("virtual compatibility coverage at height %d", rung.AppliedHeight)
			if rung.SubsumedByVersion != 0 {
				state += fmt.Sprintf(" (subsumed by app-v%d)", rung.SubsumedByVersion)
			}
		} else if rung.Valid {
			state = fmt.Sprintf("height %d", rung.AppliedHeight)
		} else if rung.Present {
			state = "INVALID: " + rung.Problem
		}
		fmt.Printf("  app-v%-2d %s\n", rung.Version, state)
	}
	return nil
}

func runUpgradeLineageDoctor(args []string) error {
	fs := flag.NewFlagSet("upgrade lineage doctor", flag.ContinueOnError)
	rpc := fs.String("rpc", defaultCometRPC(), "CometBFT RPC endpoint")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	manifestOut := fs.String("manifest-out", "", "write only the canonical repair manifest to this file")
	legacyAnchor := fs.String("legacy-anchor", "", "audited JSON fallback containing independent heights and/or actual version transitions")
	ackAnchor := fs.Bool("acknowledge-unverified-anchor", false, "explicitly attest that legacy-anchor heights are not locally history-verified and require quorum review")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	status, err := readUpgradeLineageStatus(ctx, *rpc)
	if err != nil {
		return err
	}
	out := lineageDoctorOutput{Status: *status}
	missing := make(map[uint64]struct{})
	invalidPresent := false
	for _, rung := range status.Rungs {
		if rung.Present && !rung.Valid {
			invalidPresent = true
			out.Diagnostics = append(out.Diagnostics, fmt.Sprintf("%s exists but is invalid; repair is forbidden from overwriting it", rung.Name))
		}
		if !rung.Present && rung.Version <= status.CurrentAppVersion {
			missing[rung.Version] = struct{}{}
		}
	}
	if invalidPresent {
		out.Diagnostics = append(out.Diagnostics, "lineage is not repairable while any persisted rung is present but invalid")
		return emitLineageDoctor(out, *jsonOut, "")
	}
	if len(missing) == 0 {
		out.Repairable = true
		out.Diagnostics = append(out.Diagnostics, "no missing activated lineage records")
		return emitLineageDoctor(out, *jsonOut, "")
	}
	if status.CurrentAppVersion != 21 {
		out.Diagnostics = append(out.Diagnostics, "repair manifests are admitted only while the chain is exactly app-v21")
		return emitLineageDoctor(out, *jsonOut, "")
	}
	chainID, err := readCometChainID(ctx, *rpc)
	if err != nil {
		return err
	}
	retained, diagnostics := discoverRetainedLineageEvidence(ctx, *rpc, status, missing)
	evidence := retained.Claims
	transitions := retained.Transitions
	out.Diagnostics = append(out.Diagnostics, diagnostics...)
	if len(evidence)+len(transitions) > 0 {
		out.Diagnostics = append(out.Diagnostics,
			"Comet version updates and block hashes are evidence from this validator's retained local archive; every validator must reconstruct them independently")
	}

	anchorDigest := ""
	if (!retained.Complete || !exactLineageCoverage(missing, evidence, transitions)) && *legacyAnchor != "" {
		if !*ackAnchor {
			out.Diagnostics = append(out.Diagnostics, "legacy-anchor fallback requires --acknowledge-unverified-anchor because its heights cannot be verified from retained local history")
			return emitLineageDoctor(out, *jsonOut, "")
		}
		raw, readErr := os.ReadFile(*legacyAnchor) //nolint:gosec // explicit operator audit file
		if readErr != nil {
			return readErr
		}
		var anchor legacyAnchorDocument
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if decodeErr := decoder.Decode(&anchor); decodeErr != nil {
			return fmt.Errorf("decode legacy anchor: %w", decodeErr)
		}
		canonical, _ := json.Marshal(anchor)
		digest := sha256.Sum256(canonical)
		anchorDigest = hex.EncodeToString(digest[:])
		// Never blend two trust models in one manifest. When retained Comet
		// history is incomplete, the explicit legacy anchor must attest every
		// missing rung and replaces the partial local-history set wholesale.
		evidence = evidence[:0]
		transitions = transitions[:0]
		for version := range missing {
			height := anchor.Heights[strconv.FormatUint(version, 10)]
			if height > 0 {
				evidence = append(evidence, sageabci.LegacyLineageEvidence{Version: version, Name: tx.CanonicalUpgradeName(version), AppliedHeight: height, Source: "legacy-anchor"})
			}
		}
		for _, transition := range anchor.Transitions {
			transitions = append(transitions, sageabci.LineageActivationTransition{
				FromVersion: transition.FromVersion, ToVersion: transition.ToVersion,
				AppliedHeight: transition.AppliedHeight, Source: "legacy-anchor", SubsumedVersions: append([]uint64(nil), transition.SubsumedVersions...),
			})
		}
	}
	if anchorDigest == "" && !retained.Complete {
		out.Diagnostics = append(out.Diagnostics, "retained Comet history could not be replayed through the committed tip")
		return emitLineageDoctor(out, *jsonOut, "")
	}
	if !exactLineageCoverage(missing, evidence, transitions) {
		out.Diagnostics = append(out.Diagnostics, "retained Comet evidence is incomplete; provide --legacy-anchor only after an independent audit")
		return emitLineageDoctor(out, *jsonOut, "")
	}
	sort.Slice(evidence, func(i, j int) bool { return evidence[i].Version < evidence[j].Version })
	if evidenceErr := validateDoctorLineageEvidenceV2(status, evidence, transitions); evidenceErr != nil {
		out.Diagnostics = append(out.Diagnostics, evidenceErr.Error())
		return emitLineageDoctor(out, *jsonOut, "")
	}
	manifest := sageabci.LegacyLineageRepairManifest{Schema: status.Schema, ChainID: chainID, GovernanceDomain: status.GovernanceDomain, CurrentAppVersion: 21, PriorLineageDigest: status.ValidLineageDigest, AnchorDigest: anchorDigest, Evidence: evidence, Transitions: transitions}
	if anchorDigest != "" {
		manifest.AnchorAttestation = "operator-quorum-attested-unverified-history"
		out.Diagnostics = append(out.Diagnostics, "legacy-anchor claims are quorum-attested operator assertions and are not verified by retained local history")
	}
	manifestBytes, _ := json.Marshal(manifest)
	canonical, err := sageabci.CanonicalizeLegacyLineageRepair(manifestBytes)
	if err != nil {
		return err
	}
	out.Repairable = true
	out.ManualVoteRequired = true
	out.Manifest = json.RawMessage(canonical)
	manifestDigest := sha256.Sum256([]byte(canonical))
	out.ManifestDigest = hex.EncodeToString(manifestDigest[:])
	out.ReviewSteps = []string{
		"halt every validator, deploy v11.18.1 everywhere, and confirm identical v2 status before proposing or voting",
		"create the candidate manifest with upgrade lineage doctor on the proposing validator",
		"run upgrade lineage verify with that exact manifest independently on every validator and compare manifest_digest before proposing",
		"after proposing, inspect the active proposal with sage_gov_status and cast an explicit sage_gov_vote on every validator; automatic voting is disabled, including on one-validator chains",
	}
	if *manifestOut != "" {
		if err := os.WriteFile(*manifestOut, append([]byte(canonical), '\n'), 0o600); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}
	return emitLineageDoctor(out, *jsonOut, canonical)
}

func validateDoctorLineageEvidence(status *upgradeLineageRPCStatus, evidence []sageabci.LegacyLineageEvidence) error {
	proposed := make(map[uint64]int64, len(evidence))
	for _, claim := range evidence {
		if _, duplicate := proposed[claim.Version]; duplicate {
			return fmt.Errorf("duplicate app-v%d repair claim", claim.Version)
		}
		proposed[claim.Version] = claim.AppliedHeight
	}
	var previous int64
	for _, rung := range status.Rungs {
		height := rung.AppliedHeight
		if !rung.Present {
			height = proposed[rung.Version]
		}
		if height <= previous || height > status.PersistedHeight {
			return fmt.Errorf("app-v%d claimed height %d is not a retained, strictly ordered committed height", rung.Version, height)
		}
		previous = height
	}
	return nil
}

func validateDoctorLineageEvidenceV2(status *upgradeLineageRPCStatus, evidence []sageabci.LegacyLineageEvidence, transitions []sageabci.LineageActivationTransition) error {
	if len(transitions) == 0 {
		return validateDoctorLineageEvidence(status, evidence)
	}
	virtualHeight := make(map[uint64]int64)
	validatedActivationHeight := make(map[uint64]int64)
	for _, rung := range status.Rungs {
		if rung.Present && rung.Valid && !rung.Virtual {
			validatedActivationHeight[rung.Version] = rung.AppliedHeight
		}
	}
	for _, claim := range evidence {
		if _, duplicate := virtualHeight[claim.Version]; duplicate {
			return fmt.Errorf("duplicate app-v%d repair claim", claim.Version)
		}
		virtualHeight[claim.Version] = claim.AppliedHeight
		validatedActivationHeight[claim.Version] = claim.AppliedHeight
	}
	var previousTransitionHeight int64
	for _, transition := range transitions {
		if transition.FromVersion >= transition.ToVersion || transition.ToVersion > 21 || transition.AppliedHeight <= previousTransitionHeight || transition.AppliedHeight > status.PersistedHeight || len(transition.SubsumedVersions) == 0 {
			return errors.New("retained version transitions are invalid, future-dated, or not strictly ordered")
		}
		switch transition.Source {
		case "legacy-anchor":
			if transition.BlockHash != "" {
				return errors.New("legacy-anchor transition must not claim a block hash")
			}
		case "comet-block-results":
			decoded, err := hex.DecodeString(transition.BlockHash)
			if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != transition.BlockHash {
				return errors.New("retained-Comet transition requires a canonical block hash")
			}
		default:
			return errors.New("unsupported transition evidence source")
		}
		previousTransitionHeight = transition.AppliedHeight
		if transition.FromVersion >= 6 {
			sourceHeight, sourceOK := validatedActivationHeight[transition.FromVersion]
			if !sourceOK || sourceHeight >= transition.AppliedHeight {
				return fmt.Errorf("app-v%d transition source is not a prior validated activation at a strictly earlier height", transition.FromVersion)
			}
		}
		var targetOK, targetMissing bool
		for _, rung := range status.Rungs {
			if rung.Version == transition.ToVersion {
				targetMissing = !rung.Present
				targetOK = rung.Present && rung.Valid && rung.AppliedHeight == transition.AppliedHeight
				break
			}
		}
		if !targetOK && (!targetMissing || transition.ToVersion > 19) {
			return fmt.Errorf("app-v%d is not a real canonical target activation at height %d", transition.ToVersion, transition.AppliedHeight)
		}
		if targetMissing {
			if _, duplicate := virtualHeight[transition.ToVersion]; duplicate {
				return fmt.Errorf("duplicate app-v%d repair claim", transition.ToVersion)
			}
			virtualHeight[transition.ToVersion] = transition.AppliedHeight
		}
		validatedActivationHeight[transition.ToVersion] = transition.AppliedHeight
		var previousVersion uint64
		expectedSubsumed := make([]uint64, 0)
		for version := transition.FromVersion + 1; version < transition.ToVersion; version++ {
			for _, rung := range status.Rungs {
				if rung.Version == version && !rung.Present {
					expectedSubsumed = append(expectedSubsumed, version)
					break
				}
			}
		}
		if len(expectedSubsumed) != len(transition.SubsumedVersions) {
			return errors.New("transition subsumed_versions is not the exact missing open-interval predecessor set")
		}
		for _, version := range transition.SubsumedVersions {
			if version <= transition.FromVersion || version >= transition.ToVersion || (previousVersion != 0 && version <= previousVersion) {
				return errors.New("transition subsumed_versions is not a strictly ordered open-interval subset")
			}
			if _, duplicate := virtualHeight[version]; duplicate {
				return fmt.Errorf("duplicate app-v%d repair claim", version)
			}
			virtualHeight[version] = transition.AppliedHeight
			previousVersion = version
		}
		for i := range expectedSubsumed {
			if expectedSubsumed[i] != transition.SubsumedVersions[i] {
				return errors.New("transition subsumed_versions is not the exact missing open-interval predecessor set")
			}
		}
	}
	var previousHeight int64
	for _, rung := range status.Rungs {
		height := rung.AppliedHeight
		if !rung.Present {
			height = virtualHeight[rung.Version]
		}
		if height <= 0 || height > status.PersistedHeight || height < previousHeight {
			return fmt.Errorf("app-v%d height %d is not a retained, monotonically ordered committed height", rung.Version, height)
		}
		previousHeight = height
	}
	return nil
}

func emitLineageDoctor(out lineageDoctorOutput, jsonOut bool, canonical string) error {
	if jsonOut {
		return writeJSONStdout(out)
	}
	for _, line := range out.Diagnostics {
		fmt.Println(line)
	}
	if canonical != "" {
		fmt.Printf("Repair manifest digest: %s\n", out.ManifestDigest)
		fmt.Println("Repair manifest is ready; propose it explicitly with --lineage-repair.")
		fmt.Println("Automatic voting is disabled. Every validator must independently review and explicitly vote.")
	}
	return nil
}

func writeJSONStdout(value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		fmt.Println(string(encoded))
	}
	return err
}

func readUpgradeLineageStatus(ctx context.Context, rpc string) (*upgradeLineageRPCStatus, error) {
	raw, err := readABCIQueryValue(ctx, rpc, "/upgrade/lineage")
	if err != nil {
		return nil, err
	}
	var status upgradeLineageRPCStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func readABCIQueryValue(ctx context.Context, rpc, path string) ([]byte, error) {
	queryURL := strings.TrimRight(rpc, "/") + "/abci_query?path=" + url.QueryEscape(strconv.Quote(path))
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Result struct {
			Response struct {
				Code  uint32 `json:"code"`
				Log   string `json:"log"`
				Value string `json:"value"`
			} `json:"response"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Error != nil || out.Result.Response.Code != 0 {
		return nil, fmt.Errorf("ABCI query %s rejected: %s", path, out.Result.Response.Log)
	}
	return base64.StdEncoding.DecodeString(out.Result.Response.Value)
}

func discoverRetainedLineageEvidence(ctx context.Context, rpc string, status *upgradeLineageRPCStatus, missing map[uint64]struct{}) (retainedLineageEvidence, []string) {
	result := retainedLineageEvidence{Complete: true}
	diagnostics := []string{}
	current := uint64(1)
	for height := int64(1); height <= status.PersistedHeight; height++ {
		version, err := readBlockResultAppVersion(ctx, rpc, height)
		if err != nil {
			result.Complete = false
			diagnostics = append(diagnostics, fmt.Sprintf("retained block-results unavailable at height %d: %v", height, err))
			break
		}
		if version == 0 {
			continue
		}
		if version <= current {
			result.Complete = false
			diagnostics = append(diagnostics, fmt.Sprintf("ambiguous retained version history at height %d: app-v%d follows app-v%d", height, version, current))
			break
		}

		var targetPresent bool
		for _, rung := range status.Rungs {
			if rung.Version == version && rung.Present && rung.Valid && rung.AppliedHeight == height {
				targetPresent = true
				break
			}
		}
		subsumed := make([]uint64, 0)
		for candidate := current + 1; candidate < version; candidate++ {
			if candidate > 19 {
				break
			}
			if _, wanted := missing[candidate]; wanted {
				subsumed = append(subsumed, candidate)
			}
		}
		_, direct := missing[version]
		if len(subsumed) > 0 && (targetPresent || (direct && version <= 19)) {
			hash, hashErr := readBlockHash(ctx, rpc, height)
			if hashErr != nil {
				result.Complete = false
				diagnostics = append(diagnostics, fmt.Sprintf("block hash unavailable at height %d: %v", height, hashErr))
			} else {
				result.Transitions = append(result.Transitions, sageabci.LineageActivationTransition{
					FromVersion: current, ToVersion: version, AppliedHeight: height,
					Source: "comet-block-results", BlockHash: hash, SubsumedVersions: subsumed,
				})
			}
		} else if direct {
			hash, hashErr := readBlockHash(ctx, rpc, height)
			if hashErr != nil {
				result.Complete = false
				diagnostics = append(diagnostics, fmt.Sprintf("block hash unavailable at height %d: %v", height, hashErr))
			} else {
				result.Claims = append(result.Claims, sageabci.LegacyLineageEvidence{
					Version: version, Name: tx.CanonicalUpgradeName(version), AppliedHeight: height,
					Source: "comet-block-results", BlockHash: hash,
				})
			}
		}
		current = version
	}
	sort.Slice(result.Claims, func(i, j int) bool { return result.Claims[i].Version < result.Claims[j].Version })
	return result, diagnostics
}

func exactLineageCoverage(missing map[uint64]struct{}, claims []sageabci.LegacyLineageEvidence, transitions []sageabci.LineageActivationTransition) bool {
	seen := make(map[uint64]struct{}, len(missing))
	for _, claim := range claims {
		if _, wanted := missing[claim.Version]; !wanted || claim.Version > 19 || claim.Name != tx.CanonicalUpgradeName(claim.Version) || claim.AppliedHeight <= 0 {
			return false
		}
		if _, duplicate := seen[claim.Version]; duplicate {
			return false
		}
		seen[claim.Version] = struct{}{}
	}
	for _, transition := range transitions {
		for _, version := range transition.SubsumedVersions {
			if _, wanted := missing[version]; !wanted || version > 19 {
				return false
			}
			if _, duplicate := seen[version]; duplicate {
				return false
			}
			seen[version] = struct{}{}
		}
		if _, targetMissing := missing[transition.ToVersion]; targetMissing {
			if transition.ToVersion > 19 {
				return false
			}
			if _, duplicate := seen[transition.ToVersion]; duplicate {
				return false
			}
			seen[transition.ToVersion] = struct{}{}
		}
	}
	return len(seen) == len(missing)
}

func validAnchorTrustBundle(status *upgradeLineageRPCStatus, claims []sageabci.LegacyLineageEvidence, transitions []sageabci.LineageActivationTransition) bool {
	for _, claim := range claims {
		if claim.Source != "legacy-anchor" || claim.BlockHash != "" {
			return false
		}
	}
	for _, transition := range transitions {
		if transition.Source != "legacy-anchor" || transition.BlockHash != "" {
			return false
		}
	}
	return validateDoctorLineageEvidenceV2(status, claims, transitions) == nil
}

func anchorDigestMatchesManifest(manifest sageabci.LegacyLineageRepairManifest) bool {
	anchor := legacyAnchorDocument{Heights: make(map[string]int64)}
	for _, claim := range manifest.Evidence {
		if claim.Source != "legacy-anchor" {
			return false
		}
		anchor.Heights[strconv.FormatUint(claim.Version, 10)] = claim.AppliedHeight
	}
	if len(anchor.Heights) == 0 {
		anchor.Heights = nil
	}
	for _, transition := range manifest.Transitions {
		if transition.Source != "legacy-anchor" {
			return false
		}
		anchor.Transitions = append(anchor.Transitions, legacyAnchorTransition{FromVersion: transition.FromVersion, ToVersion: transition.ToVersion, AppliedHeight: transition.AppliedHeight, SubsumedVersions: append([]uint64(nil), transition.SubsumedVersions...)})
	}
	raw, err := json.Marshal(anchor)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]) == manifest.AnchorDigest
}

func equalRetainedLineageEvidence(a, b retainedLineageEvidence) bool {
	aRaw, aErr := json.Marshal(a)
	bRaw, bErr := json.Marshal(b)
	return aErr == nil && bErr == nil && string(aRaw) == string(bRaw)
}

func readBlockResultAppVersion(ctx context.Context, rpc string, height int64) (uint64, error) {
	u := fmt.Sprintf("%s/block_results?height=%d", strings.TrimRight(rpc, "/"), height)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Result struct {
			Updates struct {
				Version struct {
					App string `json:"app"`
				} `json:"version"`
			} `json:"consensus_param_updates"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if out.Error != nil {
		return 0, errors.New("RPC error")
	}
	if out.Result.Updates.Version.App == "" {
		return 0, nil
	}
	return strconv.ParseUint(out.Result.Updates.Version.App, 10, 64)
}

func readBlockHash(ctx context.Context, rpc string, height int64) (string, error) {
	u := fmt.Sprintf("%s/block?height=%d", strings.TrimRight(rpc, "/"), height)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Result struct {
			BlockID struct {
				Hash string `json:"hash"`
			} `json:"block_id"`
		} `json:"result"`
	}
	if decodeErr := json.NewDecoder(resp.Body).Decode(&out); decodeErr != nil {
		return "", decodeErr
	}
	decoded, err := hex.DecodeString(strings.ToLower(out.Result.BlockID.Hash))
	if err != nil || len(decoded) != sha256.Size {
		return "", errors.New("invalid block hash")
	}
	return strings.ToLower(out.Result.BlockID.Hash), nil
}
