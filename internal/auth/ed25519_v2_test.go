package auth

import (
	"testing"
)

// The chain-qualified (X-Sig-Version=2) scheme exists to kill exactly one bug
// class: a signature minted for chain pair (A→B) verifying anywhere else.
// Every test here is one concrete replay/splice attempt.

func TestV2SignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"mode":"text","query":"hello"}`)
	nonce := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	sig := SignRequestV2(priv, "chain-a", "chain-b", "POST", "/fed/v1/query", body, 1751400000, nonce)

	if !VerifyRequestV2(pub, "chain-a", "chain-b", "POST", "/fed/v1/query", body, 1751400000, nonce, sig) {
		t.Fatal("valid v2 signature failed to verify")
	}
}

func TestV2RejectsCrossChainReplay(t *testing.T) {
	pub, priv, _ := GenerateKeypair()
	body := []byte(`{}`)
	nonce := []byte{9, 9, 9, 9}
	sig := SignRequestV2(priv, "chain-a", "chain-b", "POST", "/fed/v1/query", body, 1751400000, nonce)

	cases := []struct {
		name             string
		sender, receiver string
	}{
		{"replayed against a third chain", "chain-a", "chain-c"},
		{"reflected back at the sender", "chain-b", "chain-a"},
		{"sender spoofed", "chain-x", "chain-b"},
		{"both swapped for identity pair", "chain-b", "chain-b"},
	}
	for _, tc := range cases {
		if VerifyRequestV2(pub, tc.sender, tc.receiver, "POST", "/fed/v1/query", body, 1751400000, nonce, sig) {
			t.Errorf("%s: signature verified for (%s→%s), must fail", tc.name, tc.sender, tc.receiver)
		}
	}
}

func TestV2ChainPairEncodingIsInjective(t *testing.T) {
	pub, priv, _ := GenerateKeypair()
	body := []byte(`{}`)
	nonce := []byte{1}
	// ("ab","c") and ("a","bc") must NOT produce the same binding — the NUL
	// separators inside chainBindingHash make the encoding injective.
	sig := SignRequestV2(priv, "ab", "c", "POST", "/x", body, 1, nonce)
	if VerifyRequestV2(pub, "a", "bc", "POST", "/x", body, 1, nonce, sig) {
		t.Fatal("chain pair encoding is ambiguous: (ab,c) verified as (a,bc)")
	}
}

func TestV2AndV1AreDomainSeparated(t *testing.T) {
	pub, priv, _ := GenerateKeypair()
	body := []byte(`{"q":1}`)
	nonce := []byte{7, 7}
	ts := int64(1751400000)

	v1sig := SignRequestWithNonce(priv, "POST", "/fed/v1/query", body, ts, nonce)
	if VerifyRequestV2(pub, "chain-a", "chain-b", "POST", "/fed/v1/query", body, ts, nonce, v1sig) {
		t.Fatal("a v1 signature verified under the v2 scheme")
	}
	v2sig := SignRequestV2(priv, "chain-a", "chain-b", "POST", "/fed/v1/query", body, ts, nonce)
	if VerifyRequestWithNonce(pub, "POST", "/fed/v1/query", body, ts, nonce, v2sig) {
		t.Fatal("a v2 signature verified under the v1 scheme")
	}
}

func TestV2RejectsTamperedComponents(t *testing.T) {
	pub, priv, _ := GenerateKeypair()
	body := []byte(`{"domain":"hr.public"}`)
	nonce := []byte{5, 5, 5, 5}
	ts := int64(1751400000)
	sig := SignRequestV2(priv, "chain-a", "chain-b", "POST", "/fed/v1/query", body, ts, nonce)

	if VerifyRequestV2(pub, "chain-a", "chain-b", "POST", "/fed/v1/receipt", body, ts, nonce, sig) {
		t.Error("path tampering accepted")
	}
	if VerifyRequestV2(pub, "chain-a", "chain-b", "POST", "/fed/v1/query", []byte(`{"domain":"hr.payroll"}`), ts, nonce, sig) {
		t.Error("body tampering accepted")
	}
	if VerifyRequestV2(pub, "chain-a", "chain-b", "POST", "/fed/v1/query", body, ts+1, nonce, sig) {
		t.Error("timestamp tampering accepted")
	}
	if VerifyRequestV2(pub, "chain-a", "chain-b", "POST", "/fed/v1/query", body, ts, []byte{6, 6, 6, 6}, sig) {
		t.Error("nonce tampering accepted")
	}
}
