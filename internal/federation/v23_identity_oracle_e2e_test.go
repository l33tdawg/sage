package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

func enrollV23SourceReadAllAdmin(
	t *testing.T,
	source *testChain,
	name string,
	pub ed25519.PublicKey,
	height int64,
) {
	t.Helper()
	agentID := hex.EncodeToString(pub)
	if err := source.badger.RegisterAgentWithCapabilities(
		agentID,
		name,
		store.AppV23RoleMember,
		"",
		"identity-oracle",
		"",
		height,
		store.DefaultSelfRegisteredAgentCapabilities,
	); err != nil {
		t.Fatal(err)
	}
	root, err := source.badger.GetAppV23Root()
	if err != nil || root == nil {
		t.Fatalf("source Root: state=%+v err=%v", root, err)
	}
	if err := source.badger.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
		AgentID:        agentID,
		ApprovedBy:     root.CredentialID,
		RootGeneration: root.Generation,
		Profile:        store.AppV23ProfileStandard,
		HomeDomain:     "local-" + agentID,
		Clearance:      4,
		Capabilities:   store.AgentCapabilityReadAllDomains,
		Active:         true,
		UpdatedHeight:  height + 1,
	}, store.AppV23RoleAdmin, 0, 0); err != nil {
		t.Fatal(err)
	}
}

func planV23QueryForAgent(
	t *testing.T,
	source, destination *testChain,
	agentID, domain string,
) *RecallPlan {
	t.Helper()
	plan, err := source.mgr.PlanRecall(
		context.Background(),
		[]string{destination.chainID},
		agentID,
		domain,
	)
	if err != nil {
		t.Fatalf("plan recall for %s: %v", agentID, err)
	}
	if len(plan.Destinations) != 1 || plan.Destinations[0] != destination.chainID {
		t.Fatalf("plan recall for %s: %+v", agentID, plan)
	}
	return plan
}

func signV23QueryAs(
	t *testing.T,
	agentKey ed25519.PrivateKey,
	plan *RecallPlan,
	request *QueryRequest,
) *QueryRequest {
	t.Helper()
	path := "/v1/memory/search"
	switch request.Mode {
	case ModeSemantic:
		path = "/v1/memory/query"
	case ModeHybrid:
		path = "/v1/memory/hybrid"
	}
	signedBody := map[string]any{
		"query": request.Query, "embedding": request.Embedding,
		"domain_tag": request.DomainTag, "provider": request.Provider,
		"min_confidence": request.MinConfidence, "top_k": request.TopK,
		"tags": request.Tags, "federated": true,
		"federate_chains": plan.Destinations,
		"federation_context": map[string]any{
			"source_chain_id":    plan.SourceChainID,
			"agreement_bindings": plan.AgreementBindings,
			"query_challenges":   plan.QueryChallenges,
		},
	}
	if request.EmbeddingProvider != "" {
		signedBody["embedding_provider"] = request.EmbeddingProvider
	}
	body, err := json.Marshal(signedBody)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	pub := agentKey.Public().(ed25519.PublicKey)
	request.AgentProof = &QueryAgentProof{
		AgentID: hex.EncodeToString(pub),
		Signature: auth.SignRequestWithNonce(
			agentKey, http.MethodPost, path, body, now, nonce,
		),
		Timestamp:        now,
		Nonce:            nonce,
		CanonicalRequest: append([]byte("POST "+path+"\n"), body...),
	}
	request.PlanAgreementBindings = plan.AgreementBindings
	request.PlanChallenges = plan.QueryChallenges
	return request
}

func TestV23FederatedGuestIdentityOracleOverTwoSAGEMTLS(t *testing.T) {
	source := newTestChain(t, "identity-source")
	destination := newTestChain(t, "identity-destination")
	agentXPub, agentXKey, agentXKeyErr := ed25519.GenerateKey(rand.Reader)
	if agentXKeyErr != nil {
		t.Fatal(agentXKeyErr)
	}
	agentYPub, agentYKey, agentYKeyErr := ed25519.GenerateKey(rand.Reader)
	if agentYKeyErr != nil {
		t.Fatal(agentYKeyErr)
	}
	agentXID := hex.EncodeToString(agentXPub)
	agentYID := hex.EncodeToString(agentYPub)
	operatorID := hex.EncodeToString(source.agentPub)
	const domain = "shared.notes"

	insertCommitted(t, destination, "identity-memory", domain,
		"only the exact linked remote agent may read this sentinel")

	listener := startListener(t, destination)
	federate(t, destination, source, "https://unused.invalid", []string{"shared"}, 4, 0)
	federate(t, source, destination, listener.URL, []string{"shared"}, 4, 0)
	enableV23Pair(t, source, destination, []string{"shared"})

	// Hold the source-side REST preflight constant. X and Y are reviewed local
	// Admins and the source operator is current Root; all three have clearance
	// 4 and the same positive central-policy Read decision for the exact domain.
	enrollV23SourceReadAllAdmin(t, source, "agent-x", agentXPub, 2)
	enrollV23SourceReadAllAdmin(t, source, "agent-y", agentYPub, 4)
	if err := source.badger.RegisterDomain(domain, operatorID, "", 6); err != nil {
		t.Fatal(err)
	}
	for _, principal := range []struct {
		name string
		id   string
	}{
		{name: "linked agent X", id: agentXID},
		{name: "unlinked agent Y", id: agentYID},
		{name: "source peer operator", id: operatorID},
	} {
		decision, authErr := source.badger.AuthorizeAppV23LocalDomain(
			principal.id, domain, store.AppV23VerbRead, false,
		)
		if authErr != nil || !decision.Allowed || decision.ExplicitDeny {
			t.Fatalf("%s did not have identical source Read preflight: decision=%+v err=%v",
				principal.name, decision, authErr)
		}
		enrollment, enrollmentErr := source.badger.GetAppV23Enrollment(principal.id)
		if enrollmentErr != nil || enrollment == nil || enrollment.Clearance != 4 {
			t.Fatalf("%s source clearance: enrollment=%+v err=%v",
				principal.name, enrollment, enrollmentErr)
		}
	}

	// Destination state differs in exactly one fact: only X is attached as a
	// linked reader. The outer peer operator, mTLS listener, active agreement,
	// peer Read policy, domain, clearance, and transport path remain identical.
	addV23TestGuest(t, destination, source, agentXID, domain, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, denied := range []struct {
		name string
		id   string
		key  ed25519.PrivateKey
	}{
		{name: "unlinked agent Y", id: agentYID, key: agentYKey},
		{name: "source peer operator", id: operatorID, key: source.agentKey},
	} {
		t.Run(denied.name+" plan is denied remotely", func(t *testing.T) {
			plan, planErr := source.mgr.PlanRecall(
				ctx, []string{destination.chainID}, denied.id, domain,
			)
			if planErr != nil {
				t.Fatal(planErr)
			}
			if len(plan.Destinations) != 0 {
				t.Fatalf("unlinked identity received a destination plan: %+v", plan)
			}
			const exactPlanDenial = `peer returned 403: {"error":"remote agent has no active linked-reader grant for this domain"}`
			if got := strings.TrimSpace(plan.Errors[destination.chainID]); got != exactPlanDenial {
				t.Fatalf("remote plan denial = %q, want %q", got, exactPlanDenial)
			}
		})

		t.Run(denied.name+" query is denied by destination guest identity", func(t *testing.T) {
			// Use fresh destination-issued state obtained for linked X, then
			// adversarially sign the exact same request as Y/the peer operator.
			// This reaches the same /fed/v1/query endpoint and proves that outer
			// operator authentication cannot substitute for the nested identity.
			plan := planV23QueryForAgent(t, source, destination, agentXID, domain)
			query := signV23QueryAs(t, denied.key, plan, &QueryRequest{
				Mode: ModeText, Query: "linked remote agent", DomainTag: domain, TopK: 5,
			})
			if _, queryErr := source.mgr.QueryPeer(ctx, destination.chainID, query); queryErr == nil {
				t.Fatal("unlinked identity read through X's destination state")
			} else {
				const exactQueryDenial = `peer identity-destination returned 403: {"error":"remote agent has no active guest link for this domain and agreement generation"}`
				if got := strings.TrimSpace(queryErr.Error()); got != exactQueryDenial {
					t.Fatalf("remote query denial = %q, want %q", got, exactQueryDenial)
				}
			}
		})
	}

	linkedPlan := planV23QueryForAgent(t, source, destination, agentXID, domain)
	linkedQuery := signV23QueryAs(t, agentXKey, linkedPlan, &QueryRequest{
		Mode: ModeText, Query: "linked remote agent", DomainTag: domain, TopK: 5,
	})
	response, err := source.mgr.QueryPeer(ctx, destination.chainID, linkedQuery)
	if err != nil {
		t.Fatalf("exact linked X recall: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].MemoryID != "identity-memory" {
		t.Fatalf("exact linked X result: %+v", response.Results)
	}

	// A linked-reader row is not local enrollment and carries no mutation or
	// governance fields. All three remote identities remain non-principals on
	// the destination, and the real authenticated write route stays a typed 501.
	guestType := reflect.TypeOf(store.FederatedGroupGuest{})
	for _, field := range []string{"Write", "Copy", "Modify", "Claim", "Govern"} {
		if _, present := guestType.FieldByName(field); present {
			t.Fatalf("federated guest schema unexpectedly grants %s", field)
		}
	}
	for _, id := range []string{agentXID, agentYID, operatorID} {
		enrollment, enrollmentErr := destination.badger.GetAppV23Enrollment(id)
		if enrollmentErr != nil || enrollment != nil {
			t.Fatalf("remote identity became a local destination principal: id=%s enrollment=%+v err=%v",
				id, enrollment, enrollmentErr)
		}
		for _, verb := range []store.AppV23DomainVerb{
			store.AppV23VerbWrite,
			store.AppV23VerbModify,
		} {
			decision, authErr := destination.badger.AuthorizeAppV23LocalDomain(
				id, domain, verb, false,
			)
			if authErr == nil && decision.Allowed {
				t.Fatalf("remote identity gained destination mutation: id=%s verb=%d decision=%+v",
					id, verb, decision)
			}
		}
		if _, writeErr := source.mgr.WritePeer(ctx, destination.chainID, &RemoteWriteRequest{
			Headers: RemoteWriteHeaders{AgentID: id},
		}); !errors.Is(writeErr, ErrRemoteWriteCapabilityUnavailable) {
			t.Fatalf("remote write preflight for %s = %v", id, writeErr)
		}
	}
	agreement, err := source.mgr.ActiveAgreement(destination.chainID)
	if err != nil {
		t.Fatal(err)
	}
	writeBody, writeStatus, err := source.mgr.doPeerRequest(
		ctx, agreement, http.MethodPost, "/fed/v1/write", map[string]any{"agent_id": agentXID},
	)
	if err != nil {
		t.Fatal(err)
	}
	const exactWriteDenial = `{"error":"federation write requires a consensus-bound ingress capability and is unavailable in the current protocol"}`
	if writeStatus != http.StatusNotImplemented ||
		strings.TrimSpace(string(writeBody)) != exactWriteDenial {
		t.Fatalf("authenticated remote write = status %d body %q",
			writeStatus, strings.TrimSpace(string(writeBody)))
	}
	for _, path := range []string{"/fed/v1/domain/claim", "/fed/v1/governance/propose"} {
		_, status, routeErr := source.mgr.doPeerRequest(
			ctx, agreement, http.MethodPost, path, map[string]any{"agent_id": agentXID},
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if status != http.StatusNotFound {
			t.Fatalf("federated mutation route %s unexpectedly exists: status=%d", path, status)
		}
	}
}
