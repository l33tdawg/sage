//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/stretchr/testify/require"
)

func ladderPlanStatus(version uint64, activation int64) sageabci.UpgradeGovernanceStatus {
	s := sageabci.UpgradeGovernanceStatus{Schema: "sage-upgrade-governance-status/v1", CurrentAppVersion: version}
	if activation > 0 {
		s.PendingPlan = &sageabci.UpgradeGovernancePendingPlan{Name: "app-v9", TargetAppVersion: 9, ActivationHeight: activation}
	}
	return s
}

func ladderPlanServer(t *testing.T, status func(int64) sageabci.UpgradeGovernanceStatus) *httptest.Server {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/abci_query" || r.URL.Query().Get("path") != `"/upgrade/governance-status"` {
			http.Error(w, "wrong canonical query", 400)
			return
		}
		raw, err := json.Marshal(status(calls.Add(1)))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"response": map[string]any{"code": 0, "value": base64.StdEncoding.EncodeToString(raw)}}})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestLadderCanonicalPlanDelayedQuorum(t *testing.T) {
	// P=100, quorum Q=110, actual A=310. The old P+200 estimate is 300.
	var rpcs []string
	for range 4 {
		srv := ladderPlanServer(t, func(call int64) sageabci.UpgradeGovernanceStatus {
			if call == 1 {
				return ladderPlanStatus(8, 0)
			}
			return ladderPlanStatus(8, 310)
		})
		rpcs = append(rpcs, srv.URL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan, err := waitForLadderUpgradePlan(ctx, rpcs, 8, 9, 100, "", time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, int64(310), plan.ActivationHeight)
	require.Equal(t, []int64{310, 311, 312}, ladderActivationHeaders(plan.ActivationHeight))
	require.Equal(t, int64(400), ladderPostActivationEpoch(plan.ActivationHeight))
	require.Equal(t, int64(400), ladderPostActivationEpoch(399))
	require.Equal(t, int64(500), ladderPostActivationEpoch(400))
}

func TestLadderCanonicalPlanRejectsInconsistentOrMissedEvidence(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*sageabci.UpgradeGovernanceStatus)
	}{
		{"different_activation", func(s *sageabci.UpgradeGovernanceStatus) { s.PendingPlan.ActivationHeight++ }},
		{"proposal_height_guess", func(s *sageabci.UpgradeGovernanceStatus) { s.PendingPlan.ActivationHeight = 300 }},
		{"wrong_name", func(s *sageabci.UpgradeGovernanceStatus) { s.PendingPlan.Name = "app-v10" }},
		{"wrong_target", func(s *sageabci.UpgradeGovernanceStatus) { s.PendingPlan.TargetAppVersion = 10 }},
		{"wrong_domain", func(s *sageabci.UpgradeGovernanceStatus) { s.PendingPlan.GovernanceDomain = "unexpected" }},
		{"unexpected_binary", func(s *sageabci.UpgradeGovernanceStatus) { s.PendingPlan.BinarySHA256 = "unexpected" }},
		{"activated_before_observation", func(s *sageabci.UpgradeGovernanceStatus) { s.CurrentAppVersion = 9; s.PendingPlan = nil }},
		{"wrong_previous_version", func(s *sageabci.UpgradeGovernanceStatus) { s.CurrentAppVersion = 7 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			healthy := ladderPlanServer(t, func(int64) sageabci.UpgradeGovernanceStatus { return ladderPlanStatus(8, 310) })
			other := ladderPlanServer(t, func(int64) sageabci.UpgradeGovernanceStatus { s := ladderPlanStatus(8, 310); tc.modify(&s); return s })
			_, err := waitForLadderUpgradePlan(context.Background(), []string{healthy.URL, healthy.URL, healthy.URL, other.URL}, 8, 9, 100, "", time.Millisecond)
			require.Error(t, err)
		})
	}
	t.Run("missing_plan_times_out", func(t *testing.T) {
		srv := ladderPlanServer(t, func(int64) sageabci.UpgradeGovernanceStatus { return ladderPlanStatus(8, 0) })
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := waitForLadderUpgradePlan(ctx, []string{srv.URL, srv.URL, srv.URL, srv.URL}, 8, 9, 100, "", time.Millisecond)
		require.Error(t, err)
	})
	t.Run("observed_plan_disappears", func(t *testing.T) {
		first := ladderPlanServer(t, func(call int64) sageabci.UpgradeGovernanceStatus {
			if call == 1 {
				return ladderPlanStatus(8, 310)
			}
			return ladderPlanStatus(8, 0)
		})
		missing := ladderPlanServer(t, func(int64) sageabci.UpgradeGovernanceStatus { return ladderPlanStatus(8, 0) })
		_, err := waitForLadderUpgradePlan(context.Background(), []string{first.URL, missing.URL, missing.URL, missing.URL}, 8, 9, 100, "", time.Millisecond)
		require.ErrorContains(t, err, "disappeared")
	})
}

func TestLadderCanonicalPlanQueryFailsClosed(t *testing.T) {
	valid, _ := json.Marshal(ladderPlanStatus(8, 310))
	encoded := base64.StdEncoding.EncodeToString(valid)
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"http_error", 503, "unavailable"},
		{"rpc_error", 200, `{"error":{"code":-1,"message":"failed"}}`},
		{"abci_error", 200, fmt.Sprintf(`{"result":{"response":{"code":1,"value":%q}}}`, encoded)},
		{"missing_response", 200, `{"result":{}}`},
		{"missing_value", 200, `{"result":{"response":{"code":0}}}`},
		{"bad_base64", 200, `{"result":{"response":{"code":0,"value":"!"}}}`},
		{"bad_json", 200, `not JSON`},
		{"bad_payload_json", 200, `{"result":{"response":{"code":0,"value":"IQ=="}}}`},
		{"wrong_schema", 200, `{"result":{"response":{"code":0,"value":"e30="}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			_, err := readLadderUpgradeStatus(context.Background(), srv.URL)
			require.Error(t, err)
		})
	}
}

func TestLadderCanonicalPlanLegacyAndDomain(t *testing.T) {
	for _, tc := range []struct {
		name             string
		previous, target uint64
		activation       int64
		domain           string
	}{
		{"legacy", 5, 6, 300, ""},
		{"governance_domain", 19, 20, 310, "chain-domain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := ladderPlanServer(t, func(int64) sageabci.UpgradeGovernanceStatus {
				s := ladderPlanStatus(tc.previous, tc.activation)
				s.PendingPlan.Name = fmt.Sprintf("app-v%d", tc.target)
				s.PendingPlan.TargetAppVersion = tc.target
				s.PendingPlan.GovernanceDomain = tc.domain
				return s
			})
			plan, err := waitForLadderUpgradePlan(context.Background(), []string{srv.URL, srv.URL, srv.URL, srv.URL}, tc.previous, tc.target, 100, tc.domain, time.Millisecond)
			require.NoError(t, err)
			require.Equal(t, tc.activation, plan.ActivationHeight)
		})
	}
}
