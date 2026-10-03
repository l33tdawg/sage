# v15 qualification on SAGE's pinned runtime

Measured 2026-10-03 on darwin/arm64 with the actual SAGE Go `LocalClient`,
Ollama v0.31.1, and the pinned v15 Q8_0 GGUF. The 126 support and 80 lasting
seed pairs have digest `a8e5f4ad30a56dc9`. Every item was scored twice with
answer-order debias both disabled and enabled. Metrics use this bench's existing
qualification criteria and a 0.9 acceptance threshold.

| Check | Debias | AUROC | ECE | Accepted genuine | Accepted traps | Precision | Repeat flip rate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| support | off | 0.9941 | 0.0358 | 32/36 | 1/90 | 0.9697 | 0 |
| lasting | off | 0.9993 | 0.0881 | 36/49 | 0/31 | 1.0000 | 0 |
| support | on | 0.9944 | 0.0400 | 32/36 | 1/90 | 0.9697 | 0 |
| lasting | on | 1.0000 | 0.1041 | 34/49 | 0/31 | 1.0000 | 0 |

All four runs meet the bench's qualification and usefulness bars. The support
trap `s102` passes in both modes. This measurement does not reproduce the
contributor's zero-trap support result on Ollama 0.34.4. It qualifies the public
seed set on this runtime and platform; the contributor's private fresh
holdouts, training-data provenance, other platforms, and multilingual robustness
were not independently remeasured here. The feature remains experimental and
off by default.

The runtime archive and original GGUF were downloaded and verified using
SAGE's stock installers in a separate temporary directory. Ollama rewrites a
GGUF during import, so the source download and the registered inference blob
have separate pins:

- download: `714ff9324133ba3b7166fe82fa362226fae5574c4f9cfbc53c054248b16f2cfd`
- registered blob: `86bc739212ea59719839a59a1b994d720f2c9ec4010c32c8d92800db7b465169`

The full inference run used a macOS `sandbox-exec` policy denying outbound
networking except localhost. An external HTTPS probe failed inside that
sandbox. HTTP_PROXY, HTTPS_PROXY and ALL_PROXY pointed to unused loopback port
9. Inherited OLLAMA_NO_CLOUD=0 was overridden to 1 by the managed child. This
used real inference, with no mocked model endpoint. Downloads happened before
network containment was enabled.

[Per-item results](../results/sage-judge-v15-ollama-0.31.1-go-arm64.json) include
both repetitions, categories, metrics, and qualification decisions.

To reproduce the Go scores in an isolated directory:

```sh
SAGE_JUDGE_QUALIFICATION_DIR=/tmp/sage-judge-qualification \
  go test ./internal/ollamad -run '^TestJudgeQualificationPinnedRuntime$' \
  -v -count=1 -timeout 50m
```

This opt-in integration test downloads the pinned runtime and model on its first
run, uses a separate free loopback port, writes `qualification-scores.jsonl`, and
stops its own sidecar afterward. Apply OS network containment after the initial
downloads to reproduce the offline phase.
