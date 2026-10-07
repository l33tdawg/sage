//go:build sage_judge_qualification

package ollamad

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/l33tdawg/sage/internal/hunch"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJudgeQualificationPinnedRuntime(t *testing.T) {
	dir := os.Getenv("SAGE_JUDGE_QUALIFICATION_DIR")
	if dir == "" {
		t.Skip("set SAGE_JUDGE_QUALIFICATION_DIR to a separate test directory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	m := New(dir)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m.port = l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	if m.Probe(ctx) {
		t.Fatal("reserved qualification port already serves Ollama")
	}
	t.Log("installing pinned runtime", engineRelease, "in", dir)
	last := time.Now()
	if err := m.InstallEngine(ctx, func(done, total int64) {
		if time.Since(last) > 10*time.Second || done == total {
			t.Logf("runtime download %d/%d", done, total)
			last = time.Now()
		}
	}); err != nil {
		t.Fatal(err)
	}
	url, err := m.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	t.Log("isolated runtime", url)
	if err := m.EnsureJudgeModel(ctx, func(s string) { t.Log(s) }); err != nil {
		t.Fatal(err)
	}
	t.Log("verified and registered GGUF", JudgeGGUFSHA256)
	out, err := os.Create(filepath.Join(dir, "qualification-scores.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	for _, axis := range []string{"support", "lasting"} {
		f, err := os.Open(filepath.Join("../../bench/judge-qualify/pairs", axis+".jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		scan := bufio.NewScanner(f)
		for scan.Scan() {
			var item struct {
				ID       string `json:"id"`
				Category string `json:"category"`
				Memory   string `json:"memory"`
				Text     string `json:"text"`
				Evidence string `json:"evidence"`
				Label    bool   `json:"label"`
			}
			if err := json.Unmarshal(scan.Bytes(), &item); err != nil {
				t.Fatal(err)
			}
			if (axis == "support" && (item.Memory == "" || item.Evidence == "")) || (axis == "lasting" && item.Text == "") {
				t.Fatal("qualification fixture has empty input", axis, item.ID)
			}
			for _, debias := range []bool{false, true} {
				client := &hunch.LocalClient{BaseURL: url + "/v1", Model: JudgeModelTag, ExpectedGGUFSHA256: JudgeModelBlobSHA256, Debias: debias, Timeout: 2 * time.Minute}
				probabilities := []float64{}
				for repeat := 0; repeat < 2; repeat++ {
					var p float64
					var err error
					if axis == "support" {
						p, err = (hunch.SupportJudge{Client: client}).SupportedProbability(ctx, item.Memory, item.Evidence)
					} else {
						p, err = (hunch.LastingJudge{Client: client}).LastingProbability(ctx, item.Text)
					}
					if err != nil {
						t.Fatalf("%s %s debias=%v: %v", axis, item.ID, debias, err)
					}
					probabilities = append(probabilities, p)
				}
				if err := enc.Encode(map[string]any{"axis": axis, "id": item.ID, "category": item.Category, "label": item.Label, "debias": debias, "p": probabilities}); err != nil {
					t.Fatal(err)
				}
			}
			t.Log("scored", axis, item.ID)
		}
		if err := scan.Err(); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
}
