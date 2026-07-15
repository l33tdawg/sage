package scope

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validRecord() Record {
	return Record{
		ScopeID:               "audit-scope",
		Revision:              3,
		State:                 StateActive,
		ControllerValidatorID: "validator-a",
		CreatedHeight:         100,
		UpdatedHeight:         140,
		Domains:               []Domain{{Name: "audit"}, {Name: "research"}},
		Members: []Member{
			{ValidatorID: "validator-a", AssignedWeight: 4, JoinedRevision: 1, Active: true},
			{ValidatorID: "validator-b", AssignedWeight: 3, JoinedRevision: 3, Active: true},
		},
	}
}

func TestRecordCodecRoundTripIsCanonical(t *testing.T) {
	record := validRecord()
	encoded, err := Encode(record)
	require.NoError(t, err)
	decoded, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, record, decoded)

	encodedAgain, err := Encode(decoded)
	require.NoError(t, err)
	assert.Equal(t, encoded, encodedAgain, "a valid roster has exactly one wire form")
}

func TestRecordCodecRejectsNonCanonicalOrUnsafeInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Record)
	}{
		{"unsorted domains", func(r *Record) { r.Domains[0], r.Domains[1] = r.Domains[1], r.Domains[0] }},
		{"duplicate member", func(r *Record) { r.Members[1].ValidatorID = r.Members[0].ValidatorID }},
		{"zero weight", func(r *Record) { r.Members[0].AssignedWeight = 0 }},
		{"inactive controller", func(r *Record) { r.Members[0].Active = false }},
		{"subtree not enabled in v1", func(r *Record) { r.Domains[0].Subtree = true }},
		{"joined after revision", func(r *Record) { r.Members[1].JoinedRevision = r.Revision + 1 }},
		{"scope id is not one path segment", func(r *Record) { r.ScopeID = "org/research" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRecord()
			tt.mutate(&r)
			_, err := Encode(r)
			assert.Error(t, err)
		})
	}
}

func TestRecordCodecRejectsUnknownVersionAndTrailingBytes(t *testing.T) {
	encoded, err := Encode(validRecord())
	require.NoError(t, err)

	unknown := append([]byte(nil), encoded...)
	unknown[0] = 2
	_, err = Decode(unknown)
	assert.Error(t, err)

	trailing := append(append([]byte(nil), encoded...), 0)
	_, err = Decode(trailing)
	assert.Error(t, err)
}

func FuzzDecodeRecord(f *testing.F) {
	canonical, err := Encode(validRecord())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add([]byte("not a scope record"))
	f.Fuzz(func(t *testing.T, input []byte) {
		record, decodeErr := Decode(input)
		if decodeErr != nil {
			return
		}
		reencoded, encodeErr := Encode(record)
		if encodeErr != nil {
			t.Fatalf("decoded record failed validation on re-encode: %v", encodeErr)
		}
		if !bytes.Equal(reencoded, input) {
			t.Fatal("accepted scope record has a second wire encoding")
		}
	})
}
