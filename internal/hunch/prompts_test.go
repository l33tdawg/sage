package hunch

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The fixtures were rendered by Hunch's own Python implementation (hunch/prompts.py). If this test
// fails, the Go renderer no longer asks the question the model was calibrated on.
func TestPrompts_MatchHunchByteForByte(t *testing.T) {
	f, err := os.Open("testdata_prompts.txt")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	type fixture struct {
		Context      map[string]string `json:"context"`
		Check        Check             `json:"check"`
		NFirst       bool              `json:"n_first"`
		ContextText  string            `json:"context_text"`
		QuestionText string            `json:"question_text"`
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	n := 0
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var fx fixture
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &fx))
		n++
		block := JudgeContext{Memory: fx.Context["memory"], Evidence: fx.Context["evidence"]}
		require.Equal(t, fx.ContextText, "CONTEXT:\n"+renderContext(block), "context block differs from Hunch")
		require.Equal(t, fx.QuestionText, yesNoQuestion(fx.Check, fx.NFirst), "question block differs from Hunch")
	}
	require.NoError(t, scanner.Err())
	require.GreaterOrEqual(t, n, 6, "expected the golden fixtures to cover both answer orders")
}
