package web

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/stretchr/testify/require"
)

func upgradeStatusServer(t *testing.T, status sageabci.UpgradeGovernanceStatus) *httptest.Server {
	t.Helper()
	payload, err := json.Marshal(status)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/abci_query" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if got := r.URL.Query().Get("path"); got != `"/upgrade/governance-status"` {
			t.Errorf("abci_query path = %q, want the upgrade status route", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"response": map[string]any{
			"code": 0, "value": base64.StdEncoding.EncodeToString(payload),
		}}})
	}))
	t.Cleanup(server.Close)
	return server
}

// The dashboard reads the same authoritative status the CLI does, and reports
// the two ceilings. App-v29 is compiled but dormant, so a v28 ballot is still
// carried by the auto-voter while a v29 ballot requires explicit votes.
func TestDashboardUpgradeStatusReportsCeilingsAndExplicitVoteRequirement(t *testing.T) {
	target := uint64(28)
	server := upgradeStatusServer(t, sageabci.UpgradeGovernanceStatus{
		Schema:            upgradeGovernanceStatusSchema,
		CurrentAppVersion: 27,
		ActiveProposal: &sageabci.UpgradeGovernanceActiveProposal{
			ProposalID: "proposal-28", Operation: "upgrade", TargetID: "app-v28",
			Status: "voting", TargetAppVersion: &target,
		},
	})
	handler := &DashboardHandler{CometBFTRPC: server.URL}

	recorder := httptest.NewRecorder()
	handler.handleUpgradeStatus(recorder, httptest.NewRequest(http.MethodGet, "/v1/dashboard/governance/upgrade-status", nil))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, float64(28), body["next_target_app_version"], "next rung is current+1")
	require.Equal(t, float64(sageabci.MaxSupportedAppVersion()), body["auto_vote_ceiling"])
	require.Equal(t, float64(sageabci.MaxCompiledAppVersion()), body["compiled_app_version"])
	require.Equal(t, false, body["explicit_vote_required_now"],
		"app-v28 remains within the auto-vote ceiling")
	require.Equal(t, true, body["dormant_below_ceiling_gap"])
	require.Contains(t, body, "dormant_note",
		"the note describes the compiled-ahead app-v29 gate")

	aboveCeiling := uint64(29)
	server = upgradeStatusServer(t, sageabci.UpgradeGovernanceStatus{
		Schema:            upgradeGovernanceStatusSchema,
		CurrentAppVersion: 28,
		ActiveProposal: &sageabci.UpgradeGovernanceActiveProposal{
			ProposalID: "proposal-29", Operation: "upgrade", TargetID: "app-v29",
			Status: "voting", TargetAppVersion: &aboveCeiling,
		},
	})
	handler = &DashboardHandler{CometBFTRPC: server.URL}
	recorder = httptest.NewRecorder()
	handler.handleUpgradeStatus(recorder, httptest.NewRequest(http.MethodGet, "/v1/dashboard/governance/upgrade-status", nil))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, true, body["explicit_vote_required_now"],
		"a ballot above the auto-vote ceiling can only move on explicit votes")
	require.Equal(t, true, body["dormant_below_ceiling_gap"],
		"app-v29 is compiled ahead of the app-v28 auto-vote ceiling")
}

// A chain with no ballot still reports the rung it is on and what comes next,
// so CEREBRUM can offer the propose action.
func TestDashboardUpgradeStatusOffersNextRung(t *testing.T) {
	server := upgradeStatusServer(t, sageabci.UpgradeGovernanceStatus{
		Schema:            upgradeGovernanceStatusSchema,
		CurrentAppVersion: 26,
	})
	handler := &DashboardHandler{CometBFTRPC: server.URL}

	recorder := httptest.NewRecorder()
	handler.handleUpgradeStatus(recorder, httptest.NewRequest(http.MethodGet, "/v1/dashboard/governance/upgrade-status", nil))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, float64(27), body["next_target_app_version"])
	require.Equal(t, true, body["proposable"])
	require.Equal(t, false, body["explicit_vote_required_now"])
}

// The status route is read-only: an unreachable node is a clean 503, not a
// panic and not an empty success.
func TestDashboardUpgradeStatusSurfacesUnreachableNode(t *testing.T) {
	handler := &DashboardHandler{CometBFTRPC: "http://127.0.0.1:1"}
	recorder := httptest.NewRecorder()
	handler.handleUpgradeStatus(recorder, httptest.NewRequest(http.MethodGet, "/v1/dashboard/governance/upgrade-status", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}
