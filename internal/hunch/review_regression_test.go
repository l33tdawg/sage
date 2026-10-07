package hunch

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestReview_MissingTokenLogprobIsUnavailable(t *testing.T) {
	for _, body := range []string{
		`{"choices":[{"logprobs":{"content":[{"token":"Y","top_logprobs":[]}]}}]}`,
		`{"choices":[{"logprobs":{"content":[{"token":"Y","logprob":null,"top_logprobs":[]}]}}]}`,
		`{"choices":[{"logprobs":{"content":[{"token":"Y","logprob":-0.1,"top_logprobs":[{"token":"Y"}]}]}}]}`,
	} {
		_, err := parseLabelProbability([]byte(body))
		require.Error(t, err, "missing probabilities cannot become a certain yes")
	}
}

func TestLocalReader_CloudAliasesRefusedBeforeContent(t *testing.T) {
	for _, metadata := range []string{
		`{"remote_host":"https://ollama.com","remote_model":"remote","details":{"format":"gguf"}}`,
		`{"remote_model":"remote","details":{"format":"gguf"}}`,
		`{}`,
	} {
		t.Run(metadata, func(t *testing.T) {
			var inference atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/show" {
					var got map[string]any
					require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
					require.Equal(t, map[string]any{"model": "innocent-local-alias"}, got)
					_, _ = w.Write([]byte(metadata))
					return
				}
				inference.Add(1)
				_, _ = w.Write(logprobBody("Y", -0.1, map[string]float64{"Y": -0.1}))
			}))
			defer srv.Close()
			client := &LocalClient{BaseURL: srv.URL + "/v1", Model: "innocent-local-alias"}
			_, err := SupportJudge{Client: client}.SupportedProbability(context.Background(), "private memory", "private evidence")
			require.Error(t, err)
			require.Zero(t, inference.Load(), "no memory or evidence is sent for cloud or unknown models")
		})
	}
}

func TestLocalReader_NonLoopbackEndpointRefused(t *testing.T) {
	client := &LocalClient{BaseURL: "http://192.0.2.1/v1", Model: "judge"}
	_, err := SupportJudge{Client: client}.SupportedProbability(context.Background(), "private memory", "private evidence")
	require.ErrorIs(t, err, ErrLocalOnly)
}

func TestLocalReader_ExistingManagedTagMustMatchTheGGUFPin(t *testing.T) {
	digest := "714ff9324133ba3b7166fe82fa362226fae5574c4f9cfbc53c054248b16f2cfd"
	for _, blob := range []string{"sha256-" + digest, "sha256-wrong-weights"} {
		t.Run(blob, func(t *testing.T) {
			var inference atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/show" {
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"details": map[string]string{"format": "gguf"}, "modelfile": "FROM /models/blobs/" + blob + "\n"}))
					return
				}
				inference.Add(1)
				_, _ = w.Write(logprobBody("Y", -0.1, map[string]float64{"Y": -0.1}))
			}))
			defer srv.Close()
			c := &LocalClient{BaseURL: srv.URL + "/v1", Model: "managed-judge", ExpectedGGUFSHA256: digest}
			_, err := LastingJudge{Client: c}.LastingProbability(context.Background(), "private memory")
			if blob == "sha256-"+digest {
				require.NoError(t, err)
				require.EqualValues(t, 1, inference.Load())
			} else {
				require.Error(t, err)
				require.Zero(t, inference.Load(), "a present tag alone does not establish the qualified weights")
			}
		})
	}
}
