package tx

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func functionAuthorityFixture() FunctionAuthority {
	return FunctionAuthority{Action: "assign", Function: "example-record-producer", Domain: "example-records", Scope: "example-chain", Subject: strings.Repeat("ab", 32), ExpectedRevision: 7, ExpiresAt: 2000}
}

func TestFunctionAuthorityCodecAndProofBinding(t *testing.T) {
	a := functionAuthorityFixture()
	_, key := testKeypair(t)
	parsed := &ParsedTx{Type: TxTypeFunctionAuthority, FunctionAuthority: &a, Nonce: 4, Timestamp: time.Unix(1500, 0)}
	require.NoError(t, SignTx(parsed, key))
	wire, err := EncodeTx(parsed)
	require.NoError(t, err)
	decoded, err := DecodeTxWithFunctionAuthority(wire, true)
	require.NoError(t, err)
	require.Equal(t, a, *decoded.FunctionAuthority)
	valid, err := VerifyTx(decoded)
	require.NoError(t, err)
	require.True(t, valid)
	_, err = DecodeTxWithFunctionAuthority(wire, false)
	require.ErrorIs(t, err, ErrUnknownTxType)
	bound := FunctionAuthorityActionBytes(&a)
	for _, mutate := range []func(*FunctionAuthority){
		func(v *FunctionAuthority) { v.Action = "revoke"; v.ExpiresAt = 0 },
		func(v *FunctionAuthority) { v.Function = "another-function" },
		func(v *FunctionAuthority) { v.Domain = "another-domain" },
		func(v *FunctionAuthority) { v.Scope = "another-chain" },
		func(v *FunctionAuthority) { v.Subject = strings.Repeat("cd", 32) },
		func(v *FunctionAuthority) { v.ExpectedRevision++ },
		func(v *FunctionAuthority) { v.ExpiresAt++ },
	} {
		changed := a
		mutate(&changed)
		require.NotEmpty(t, FunctionAuthorityActionBytes(&changed))
		require.NotEqual(t, bound, FunctionAuthorityActionBytes(&changed))
	}
	activation := FunctionAuthority{Action: "activate", Function: a.Function, Domain: a.Domain, Scope: a.Scope, ActivationHeight: 100}
	before := FunctionAuthorityActionBytes(&activation)
	activation.ActivationHeight++
	require.NotEqual(t, before, FunctionAuthorityActionBytes(&activation))
}

func TestFunctionAuthorityCodecClosedStructure(t *testing.T) {
	for _, action := range []string{"activate", "assign", "revoke"} {
		t.Run(action, func(t *testing.T) {
			a := functionAuthorityFixture()
			a.Action = action
			switch action {
			case "activate":
				a.Subject, a.ActivationHeight, a.ExpectedRevision, a.ExpiresAt = "", 200, 0, 0
			case "revoke":
				a.ExpiresAt = 0
			}
			wire, err := encodeFunctionAuthority(&a)
			require.NoError(t, err)
			decoded, err := decodeFunctionAuthority(wire)
			require.NoError(t, err)
			require.Equal(t, a, *decoded)
			for i := 0; i < len(wire); i++ {
				_, err := decodeFunctionAuthority(wire[:i])
				require.Error(t, err, "truncated offset %d", i)
			}
			_, err = decodeFunctionAuthority(append(bytes.Clone(wire), 0))
			require.Error(t, err)
			poisoned := bytes.Clone(wire)
			binary.BigEndian.PutUint32(poisoned[:4], math.MaxUint32)
			_, err = decodeFunctionAuthority(poisoned)
			require.Error(t, err)
		})
	}
	_, err := decodeFunctionAuthority(make([]byte, 4096))
	require.Error(t, err)
}

func TestFunctionAuthorityRefusesNoncanonicalFields(t *testing.T) {
	cases := map[string]func(*FunctionAuthority){
		"unknown action":        func(v *FunctionAuthority) { v.Action = "grant" },
		"empty function":        func(v *FunctionAuthority) { v.Function = "" },
		"oversized function":    func(v *FunctionAuthority) { v.Function = strings.Repeat("a", 65) },
		"noncanonical function": func(v *FunctionAuthority) { v.Function = "Example-records" },
		"empty domain":          func(v *FunctionAuthority) { v.Domain = "" },
		"oversized domain":      func(v *FunctionAuthority) { v.Domain = strings.Repeat("a", 513) },
		"invalid UTF8 domain":   func(v *FunctionAuthority) { v.Domain = string([]byte{255}) },
		"empty scope":           func(v *FunctionAuthority) { v.Scope = "" },
		"oversized scope":       func(v *FunctionAuthority) { v.Scope = strings.Repeat("a", 513) },
		"invalid UTF8 scope":    func(v *FunctionAuthority) { v.Scope = string([]byte{255}) },
		"missing subject":       func(v *FunctionAuthority) { v.Subject = "" },
		"uppercase subject":     func(v *FunctionAuthority) { v.Subject = strings.Repeat("AB", 32) },
		"short subject":         func(v *FunctionAuthority) { v.Subject = "ab" },
		"invalid hex subject":   func(v *FunctionAuthority) { v.Subject = strings.Repeat("zz", 32) },
		"assignment activation": func(v *FunctionAuthority) { v.ActivationHeight = 1 },
		"negative expiry":       func(v *FunctionAuthority) { v.ExpiresAt = -1 },
		"revision overflow":     func(v *FunctionAuthority) { v.ExpectedRevision = math.MaxUint64 },
		"revoke expiry":         func(v *FunctionAuthority) { v.Action = "revoke" },
		"activation subject":    func(v *FunctionAuthority) { v.Action = "activate"; v.ActivationHeight = 1 },
		"activation zero": func(v *FunctionAuthority) {
			*v = FunctionAuthority{Action: "activate", Function: v.Function, Domain: v.Domain, Scope: v.Scope}
		},
		"activation revision": func(v *FunctionAuthority) {
			*v = FunctionAuthority{Action: "activate", Function: v.Function, Domain: v.Domain, Scope: v.Scope, ActivationHeight: 1, ExpectedRevision: 1}
		},
		"activation expiry": func(v *FunctionAuthority) {
			*v = FunctionAuthority{Action: "activate", Function: v.Function, Domain: v.Domain, Scope: v.Scope, ActivationHeight: 1, ExpiresAt: 1}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			a := functionAuthorityFixture()
			mutate(&a)
			_, err := encodeFunctionAuthority(&a)
			require.Error(t, err)
			require.Nil(t, FunctionAuthorityActionBytes(&a))
		})
	}
	require.Error(t, ValidateFunctionAuthority(nil))
}

// The signed wire and shared REST decoder must reject the same ambiguous
// domains as canonical domain/grant storage, before any consensus broadcast.
func TestFunctionAuthorityDomainLexicalContract(t *testing.T) {
	for _, domain := range []string{
		":", "parent:child", ".leading", "trailing.", "empty..segment",
		"has space", "has\ttab", "has\nnewline", "has\u00a0space", "has\x00control",
		string([]byte{0xff}), strings.Repeat("a", 513), strings.Repeat("é", 257),
	} {
		t.Run("invalid_"+domain, func(t *testing.T) {
			a := functionAuthorityFixture()
			a.Domain = domain
			require.Error(t, ValidateFunctionAuthority(&a))
			_, err := encodeFunctionAuthority(&a)
			require.Error(t, err)
			require.Nil(t, FunctionAuthorityActionBytes(&a))
			// encoding/json replaces malformed UTF-8; test the actual binary
			// string above rather than pretending that replacement preserves it.
			if domain != string([]byte{0xff}) {
				body, err := json.Marshal(a)
				require.NoError(t, err)
				_, err = DecodeFunctionAuthorityRequest(body)
				require.Error(t, err)
			}
		})
	}
	for _, domain := range []string{"technical/hardware", "研发.笔记", strings.Repeat("a", 512), strings.Repeat("é", 256)} {
		t.Run("valid_"+domain, func(t *testing.T) {
			a := functionAuthorityFixture()
			a.Domain = domain
			wire, err := encodeFunctionAuthority(&a)
			require.NoError(t, err)
			decoded, err := decodeFunctionAuthority(wire)
			require.NoError(t, err)
			require.Equal(t, a, *decoded)
			body, err := json.Marshal(a)
			require.NoError(t, err)
			fromRequest, err := DecodeFunctionAuthorityRequest(body)
			require.NoError(t, err)
			require.Equal(t, a, *fromRequest)
		})
	}
}
