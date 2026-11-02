package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func functionAuthorityFixture(t *testing.T) (*BadgerStore, FunctionAuthorityPolicy, FunctionDesignation) {
	t.Helper()
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	policy := FunctionAuthorityPolicy{
		Function: "observation-writer", Domain: "lab/observations",
		ActivationHeight: 101, CreatedHeight: 100,
		CreatedBy: strings.Repeat("a1", 32), RuleVersion: 1,
	}
	designation := FunctionDesignation{
		Function: policy.Function, Domain: policy.Domain,
		Subject: strings.Repeat("b2", 32), Revision: 1,
		AssignedBy: policy.CreatedBy, RootGeneration: 1,
		UpdatedHeight: policy.CreatedHeight, ExpiresAt: 0,
	}
	return s, policy, designation
}

func TestFunctionAuthorityPolicyImmutableAndTupleIsolation(t *testing.T) {
	s, policy, _ := functionAuthorityFixture(t)
	missing, err := s.GetFunctionAuthorityPolicy(policy.Function, policy.Domain)
	require.NoError(t, err)
	require.Nil(t, missing)
	before, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.NoError(t, s.SetFunctionAuthorityPolicy(&policy))
	after, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.NotEqual(t, before, after, "policy keys must affect consensus AppHash")
	require.NoError(t, s.SetFunctionAuthorityPolicy(&policy), "identical creation replay is a no-op")
	conflict := policy
	conflict.ActivationHeight++
	require.ErrorIs(t, s.SetFunctionAuthorityPolicy(&conflict), ErrFunctionAuthorityPolicyConflict)
	got, err := s.GetFunctionAuthorityPolicy(policy.Function, policy.Domain)
	require.NoError(t, err)
	require.Equal(t, policy, *got)
	got.ActivationHeight++
	again, err := s.GetFunctionAuthorityPolicy(policy.Function, policy.Domain)
	require.NoError(t, err)
	require.Equal(t, policy, *again, "returned records must not alias process state")
	other := policy
	other.Domain = "lab.observations"
	require.NoError(t, s.SetFunctionAuthorityPolicy(&other))
	other.Function = "observation_writer"
	other.Domain = "測試/observations"
	require.NoError(t, s.SetFunctionAuthorityPolicy(&other))
	for _, p := range []FunctionAuthorityPolicy{policy, other} {
		key, err := functionAuthorityPolicyKey(p.Function, p.Domain)
		require.NoError(t, err)
		require.False(t, isIndexBackfillProgressKey(key))
		value, err := s.GetFunctionAuthorityPolicy(p.Function, p.Domain)
		require.NoError(t, err)
		require.Equal(t, p, *value)
	}
}

func TestFunctionAuthorityPolicyDomainListCanonicalOrderingAndIsolation(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	empty, err := s.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.NoError(t, err)
	require.Empty(t, empty)
	for _, function := range []string{"z-writer", policy.Function, "a-writer"} {
		p := policy
		p.Function = function
		require.NoError(t, s.SetFunctionAuthorityPolicy(&p))
		key, err := functionAuthorityPolicyKey(p.Function, p.Domain)
		require.NoError(t, err)
		require.Equal(t, "funcauth:policy:"+hex.EncodeToString([]byte(p.Domain))+":"+hex.EncodeToString([]byte(p.Function)), string(key))
	}
	for _, domain := range []string{policy.Domain + "/child", policy.Domain + "s", "lab.observations", "測試/observations"} {
		other := policy
		other.Domain = domain
		require.NoError(t, s.SetFunctionAuthorityPolicy(&other))
	}
	// A corrupt policy outside the exact domain prefix must neither be read nor
	// turn a domain-local check into a scan of every policy on the chain.
	otherKey, err := functionAuthorityPolicyKey(policy.Function, policy.Domain+"s")
	require.NoError(t, err)
	require.NoError(t, s.SetRawForTest(otherKey, []byte("not JSON")))
	listed, err := s.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.NoError(t, err)
	require.Len(t, listed, 3)
	require.Equal(t, []string{"a-writer", policy.Function, "z-writer"}, []string{listed[0].Function, listed[1].Function, listed[2].Function})
	for _, p := range listed {
		require.Equal(t, policy.Domain, p.Domain)
		got, err := s.GetFunctionAuthorityPolicy(p.Function, p.Domain)
		require.NoError(t, err)
		require.Equal(t, got, p, "point getter retains function/domain API")
	}
	listed[0].ActivationHeight++
	again, err := s.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.NoError(t, err)
	require.Equal(t, policy.ActivationHeight, again[0].ActivationHeight, "listed values do not alias process state")
	key, err := functionDesignationKey(designation.Function, designation.Domain, designation.Subject)
	require.NoError(t, err)
	require.Equal(t, "funcauth:designation:"+hex.EncodeToString([]byte(designation.Function))+":"+hex.EncodeToString([]byte(designation.Domain))+":"+designation.Subject, string(key),
		"domain-first policy layout must not silently reorder designation keys")
	for _, domain := range []string{"", "bad:domain", "bad domain"} {
		listed, err := s.ListFunctionAuthorityPoliciesForDomain(domain)
		require.Error(t, err)
		require.Nil(t, listed)
	}
}

func TestFunctionAuthorityPolicyDomainListRefusesCorruptRowsAndReadFailure(t *testing.T) {
	s, policy, _ := functionAuthorityFixture(t)
	good := policy
	good.Function = "a-good"
	require.NoError(t, s.SetFunctionAuthorityPolicy(&good))
	badKey, err := functionAuthorityPolicyKey("z-row", policy.Domain)
	require.NoError(t, err)
	raw, err := json.Marshal(policy)
	require.NoError(t, err)
	invalid := policy
	invalid.RuleVersion = 2
	invalidJSON, err := json.Marshal(invalid)
	require.NoError(t, err)
	for _, wire := range [][]byte{[]byte("null"), []byte("{"), invalidJSON, raw} {
		require.NoError(t, s.SetRawForTest(badKey, wire))
		listed, err := s.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
		require.Error(t, err)
		require.Nil(t, listed, "a validated earlier row must not escape as partial policy coverage")
	}
	closed, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, closed.CloseBadger())
	listed, err := closed.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.Error(t, err, "underlying read failure must not look like absent policy")
	require.Nil(t, listed)
}

func TestFunctionAuthorityPolicyRejectsInvalidWithoutMutation(t *testing.T) {
	s, policy, _ := functionAuthorityFixture(t)
	cases := map[string]func(*FunctionAuthorityPolicy){
		"empty-function":    func(p *FunctionAuthorityPolicy) { p.Function = "" },
		"uppercase":         func(p *FunctionAuthorityPolicy) { p.Function = "Blue" },
		"digit-start":       func(p *FunctionAuthorityPolicy) { p.Function = "1writer" },
		"delimiter":         func(p *FunctionAuthorityPolicy) { p.Function = "write:other" },
		"unicode":           func(p *FunctionAuthorityPolicy) { p.Function = "écrire" },
		"too-long":          func(p *FunctionAuthorityPolicy) { p.Function = strings.Repeat("a", 65) },
		"empty-domain":      func(p *FunctionAuthorityPolicy) { p.Domain = "" },
		"domain-delimiter":  func(p *FunctionAuthorityPolicy) { p.Domain = "lab:observations" },
		"empty-segment":     func(p *FunctionAuthorityPolicy) { p.Domain = "lab..observations" },
		"domain-whitespace": func(p *FunctionAuthorityPolicy) { p.Domain = "lab observations" },
		"negative-created":  func(p *FunctionAuthorityPolicy) { p.CreatedHeight = -1 },
		"zero-created":      func(p *FunctionAuthorityPolicy) { p.CreatedHeight = 0 },
		"same-activation":   func(p *FunctionAuthorityPolicy) { p.ActivationHeight = p.CreatedHeight },
		"past-activation":   func(p *FunctionAuthorityPolicy) { p.ActivationHeight = p.CreatedHeight - 1 },
		"missing-creator":   func(p *FunctionAuthorityPolicy) { p.CreatedBy = "" },
		"nonlocal-creator":  func(p *FunctionAuthorityPolicy) { p.CreatedBy = "peer:" + p.CreatedBy },
		"uppercase-creator": func(p *FunctionAuthorityPolicy) { p.CreatedBy = strings.Repeat("AB", 32) },
		"unsupported-rule":  func(p *FunctionAuthorityPolicy) { p.RuleVersion = 2 },
		"zero-rule":         func(p *FunctionAuthorityPolicy) { p.RuleVersion = 0 },
	}
	before, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Error(t, s.SetFunctionAuthorityPolicy(nil))
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			invalid := policy
			mutate(&invalid)
			require.Error(t, s.SetFunctionAuthorityPolicy(&invalid))
			after, err := s.ComputeAppHash()
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
	boundary := policy
	boundary.Function = "a" + strings.Repeat("z", 63)
	boundary.Domain = strings.Repeat("d", 512)
	require.NoError(t, s.SetFunctionAuthorityPolicy(&boundary))
}

func TestFunctionDesignationCASRevocationAndMetadata(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	require.ErrorIs(t, s.SetFunctionDesignation(&designation, 0), ErrFunctionAuthorityPolicyNotFound)
	require.NoError(t, s.SetFunctionAuthorityPolicy(&policy))
	require.NoError(t, s.SetFunctionDesignation(&designation, 0))
	require.ErrorIs(t, s.SetFunctionDesignation(&designation, 0), ErrFunctionDesignationRevisionConflict)
	hash1, err := s.ComputeAppHash()
	require.NoError(t, err)
	revoked := designation
	revoked.Revision = 2
	revoked.UpdatedHeight++
	revoked.Revoked = true
	require.NoError(t, s.SetFunctionDesignation(&revoked, 1))
	hash2, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash2)
	require.ErrorIs(t, s.SetFunctionDesignation(&designation, 0), ErrFunctionDesignationRevisionConflict,
		"revoked row cannot be recreated with an old expected revision")
	assigned := revoked
	assigned.Revision = 3
	assigned.Revoked = false
	assigned.AssignedBy = strings.Repeat("c3", 32)
	assigned.RootGeneration = 2
	assigned.ExpiresAt = 1700000000
	// Multiple legitimate control actions may share one consensus height.
	require.NoError(t, s.SetFunctionDesignation(&assigned, 2))
	got, err := s.GetFunctionDesignation(assigned.Function, assigned.Domain, assigned.Subject)
	require.NoError(t, err)
	require.Equal(t, assigned, *got)
	for _, change := range []func(*FunctionDesignation){
		func(d *FunctionDesignation) { d.RootGeneration = 1 },
		func(d *FunctionDesignation) { d.UpdatedHeight-- },
	} {
		regression := assigned
		regression.Revision++
		change(&regression)
		require.ErrorContains(t, s.SetFunctionDesignation(&regression, assigned.Revision), "regressed")
	}
	unchanged, err := s.GetFunctionDesignation(assigned.Function, assigned.Domain, assigned.Subject)
	require.NoError(t, err)
	require.Equal(t, assigned, *unchanged)
}

func TestFunctionDesignationRejectsInvalidAndMissingCAS(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	require.NoError(t, s.SetFunctionAuthorityPolicy(&policy))
	cases := map[string]func(*FunctionDesignation){
		"zero-revision":     func(d *FunctionDesignation) { d.Revision = 0 },
		"skipped-revision":  func(d *FunctionDesignation) { d.Revision = 2 },
		"zero-generation":   func(d *FunctionDesignation) { d.RootGeneration = 0 },
		"negative-expiry":   func(d *FunctionDesignation) { d.ExpiresAt = -1 },
		"predates-policy":   func(d *FunctionDesignation) { d.UpdatedHeight = policy.CreatedHeight - 1 },
		"zero-height":       func(d *FunctionDesignation) { d.UpdatedHeight = 0 },
		"missing-subject":   func(d *FunctionDesignation) { d.Subject = "" },
		"federated-subject": func(d *FunctionDesignation) { d.Subject = "remote:" + d.Subject },
		"uppercase-subject": func(d *FunctionDesignation) { d.Subject = strings.Repeat("AB", 32) },
		"short-subject":     func(d *FunctionDesignation) { d.Subject = strings.Repeat("ab", 31) },
		"bad-assigner":      func(d *FunctionDesignation) { d.AssignedBy = "root" },
		"bad-function":      func(d *FunctionDesignation) { d.Function = "other:function" },
		"bad-domain":        func(d *FunctionDesignation) { d.Domain = "other:domain" },
	}
	before, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Error(t, s.SetFunctionDesignation(nil, 0))
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			invalid := designation
			mutate(&invalid)
			require.Error(t, s.SetFunctionDesignation(&invalid, 0))
			after, err := s.ComputeAppHash()
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
	wrongExpected := designation
	wrongExpected.Revision = 5
	require.ErrorIs(t, s.SetFunctionDesignation(&wrongExpected, 4), ErrFunctionDesignationRevisionConflict)
	overflow := designation
	overflow.Revision = 1
	require.ErrorIs(t, s.SetFunctionDesignation(&overflow, math.MaxUint64), ErrFunctionDesignationRevisionConflict)
}

func TestFunctionAuthorityScopedCommitAndDiscard(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	before, err := s.ComputeAppHash()
	require.NoError(t, err)
	scoped := s.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.SetFunctionAuthorityPolicy(&policy))
	require.NoError(t, scoped.SetFunctionDesignation(&designation, 0))
	listed, err := scoped.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.NoError(t, err)
	require.Equal(t, []*FunctionAuthorityPolicy{&policy}, listed, "same block listing sees its earlier activation write")
	baseListed, err := s.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.NoError(t, err)
	require.Empty(t, baseListed, "base listing cannot expose uncommitted policy")
	missing, err := s.GetFunctionAuthorityPolicy(policy.Function, policy.Domain)
	require.NoError(t, err)
	require.Nil(t, missing, "base store must not expose speculative authority")
	next := designation
	next.Revision = 2
	next.Revoked = true
	require.NoError(t, scoped.SetFunctionDesignation(&next, 1), "same block sees previous ordered assignment")
	stagedHash, err := scoped.ComputeAppHash()
	require.NoError(t, err)
	require.NotEqual(t, before, stagedHash)
	scoped.DiscardConsensusTransaction()
	after, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, before, after)
	listed, err = s.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.NoError(t, err)
	require.Empty(t, listed, "discard removes speculative domain-list rows")
	scoped = s.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.SetFunctionAuthorityPolicy(&policy))
	require.NoError(t, scoped.SetFunctionDesignation(&designation, 0))
	require.NoError(t, scoped.CommitConsensusTransaction())
	listed, err = s.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.NoError(t, err)
	require.Equal(t, []*FunctionAuthorityPolicy{&policy}, listed)
	got, err := s.GetFunctionDesignation(designation.Function, designation.Domain, designation.Subject)
	require.NoError(t, err)
	require.Equal(t, designation, *got)
}

func TestFunctionAuthorityFailedWriteCannotPublishPolicyPrefix(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	scoped := s.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.SetFunctionAuthorityPolicy(&policy))
	scoped.writeFaultHook = func(int) error { return errors.New("injected function designation write failure") }
	require.ErrorContains(t, scoped.SetFunctionDesignation(&designation, 0), "injected function designation write failure")
	require.Error(t, scoped.ConsensusTransactionError())
	require.Error(t, scoped.CommitConsensusTransaction())
	missing, err := s.GetFunctionAuthorityPolicy(policy.Function, policy.Domain)
	require.NoError(t, err)
	require.Nil(t, missing, "failed designation cannot publish an earlier speculative policy")
}

func TestFunctionAuthorityIncludedInEveryExistingBadgerAppHashRule(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	hashers := []func() ([]byte, error){
		s.ComputeAppHash,
		s.ComputeAppHashExcludingState,
		s.ComputeAppHashExcludingBookkeeping,
	}
	before := make([][]byte, len(hashers))
	for i, hash := range hashers {
		var err error
		before[i], err = hash()
		require.NoError(t, err)
	}
	require.NoError(t, s.SetFunctionAuthorityPolicy(&policy))
	for i, hash := range hashers {
		after, err := hash()
		require.NoError(t, err)
		require.NotEqual(t, before[i], after, "hash rule %d must include policy", i)
		before[i] = after
	}
	require.NoError(t, s.SetFunctionDesignation(&designation, 0))
	for i, hash := range hashers {
		after, err := hash()
		require.NoError(t, err)
		require.NotEqual(t, before[i], after, "hash rule %d must include designation", i)
	}
}

func TestFunctionAuthorityBackupRestoreAndReopen(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	require.NoError(t, s.SetFunctionAuthorityPolicy(&policy))
	require.NoError(t, s.SetFunctionDesignation(&designation, 0))
	second := policy
	second.Function = "a-second-policy"
	require.NoError(t, s.SetFunctionAuthorityPolicy(&second))
	expectedHash, err := s.ComputeAppHash()
	require.NoError(t, err)
	var backup bytes.Buffer
	_, err = s.db.Backup(&backup, 0)
	require.NoError(t, err)
	path := t.TempDir()
	restored, err := NewBadgerStore(path)
	require.NoError(t, err)
	require.NoError(t, restored.db.Load(bytes.NewReader(backup.Bytes()), 16))
	require.NoError(t, restored.CloseBadger())
	restored, err = NewBadgerStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restored.CloseBadger()) })
	p, err := restored.GetFunctionAuthorityPolicy(policy.Function, policy.Domain)
	require.NoError(t, err)
	require.Equal(t, policy, *p)
	listed, err := restored.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
	require.NoError(t, err)
	require.Equal(t, []*FunctionAuthorityPolicy{&second, &policy}, listed,
		"backup/load/reopen preserves domain-first policy coverage and order")
	d, err := restored.GetFunctionDesignation(designation.Function, designation.Domain, designation.Subject)
	require.NoError(t, err)
	require.Equal(t, designation, *d)
	actualHash, err := restored.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, expectedHash, actualHash, "normal complete Badger backup/load preserves authority and AppHash")
}

func TestFunctionDesignationConcurrentCASAllowsOneWinner(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	require.NoError(t, s.SetFunctionAuthorityPolicy(&policy))
	start := make(chan struct{})
	results := make(chan error, 8)
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			results <- s.SetFunctionDesignation(&designation, 0)
		}()
	}
	close(start)
	group.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else {
			require.True(t, errors.Is(err, ErrFunctionDesignationRevisionConflict) || errors.Is(err, badger.ErrConflict), "%v", err)
		}
	}
	require.Equal(t, 1, winners)
	got, err := s.GetFunctionDesignation(designation.Function, designation.Domain, designation.Subject)
	require.NoError(t, err)
	require.Equal(t, designation, *got)
}

func TestFunctionAuthorityRefusesCorruptCanonicalState(t *testing.T) {
	s, policy, designation := functionAuthorityFixture(t)
	policyKey, err := functionAuthorityPolicyKey(policy.Function, policy.Domain)
	require.NoError(t, err)
	raw, err := json.Marshal(policy)
	require.NoError(t, err)
	wrongTuple := policy
	wrongTuple.Domain = "other-domain"
	wrongTupleJSON, err := json.Marshal(wrongTuple)
	require.NoError(t, err)
	cases := map[string][]byte{
		"null":               []byte("null"),
		"unknown":            append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"unknown":true}`)...),
		"duplicate":          append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"rule_version":1}`)...),
		"trailing":           append(append([]byte(nil), raw...), []byte(`{}`)...),
		"key-value-mismatch": wrongTupleJSON,
		"missing-field":      bytes.ReplaceAll(raw, []byte(`,"rule_version":1`), nil),
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, s.SetRawForTest(policyKey, corrupt))
			_, err := s.GetFunctionAuthorityPolicy(policy.Function, policy.Domain)
			require.Error(t, err)
			listed, err := s.ListFunctionAuthorityPoliciesForDomain(policy.Domain)
			require.Error(t, err)
			require.Nil(t, listed)
			require.Error(t, s.SetFunctionAuthorityPolicy(&policy), "must not overwrite malformed existing state")
			require.Error(t, s.SetFunctionDesignation(&designation, 0))
		})
	}
	require.NoError(t, s.SetRawForTest(policyKey, raw))
	key, err := functionDesignationKey(designation.Function, designation.Domain, designation.Subject)
	require.NoError(t, err)
	wrongSubject := designation
	wrongSubject.Subject = strings.Repeat("c3", 32)
	raw, err = json.Marshal(wrongSubject)
	require.NoError(t, err)
	require.NoError(t, s.SetRawForTest(key, raw))
	_, err = s.GetFunctionDesignation(designation.Function, designation.Domain, designation.Subject)
	require.ErrorContains(t, err, "key/value mismatch")
	require.Error(t, s.SetFunctionDesignation(&designation, 0), "CAS must not overwrite mismatched state")
}

func TestFunctionDesignationNeverInferredFromExistingRoleOrGrant(t *testing.T) {
	s, policy, _ := functionAuthorityFixture(t)
	subject := appV23Register(t, s, "function-designation-member", "member", 1, 0)
	require.NoError(t, s.RegisterDomain(policy.Domain, subject, "", 2))
	require.NoError(t, s.SetAccessGrant(policy.Domain, subject, 2, 0, subject))
	require.NoError(t, s.SetFunctionAuthorityPolicy(&policy))
	designation, err := s.GetFunctionDesignation(policy.Function, policy.Domain, subject)
	require.NoError(t, err)
	require.Nil(t, designation, "domain owner/self-grant/member standing is not an explicit designation")
}
