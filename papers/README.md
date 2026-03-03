# (S)AGE Research Papers

Research papers documenting the design, implementation, and empirical evaluation of the (S)AGE ((Sovereign) Agent Governed Experience) protocol.

## Papers

### Paper 1: Agent Memory Infrastructure
**Full Title:** Agent Memory Infrastructure: Byzantine-Resilient Institutional Memory for Multi-Agent Systems

Introduces (S)AGE — a Byzantine fault-tolerant institutional memory layer for multi-agent systems. Covers the architecture, Proof of Experience (PoE) consensus mechanism, CometBFT ABCI 2.0 integration, and the permissioned validator model. Includes performance benchmarks (956 req/s submissions, 21.6ms P95 queries) and BFT fault tolerance verification.

### Paper 2: Consensus-Validated Memory Improves Agent Performance
**Full Title:** Consensus-Validated Memory Improves Agent Performance on Complex Tasks

Presents empirical results from a controlled 50-vs-50 comparative study using the Level Up CTF platform. Demonstrates that consensus-validated institutional memory significantly improves agent performance on complex security challenge generation tasks, with statistical analysis (Mann-Whitney U, Cohen's d effect sizes).

### Paper 3: Institutional Memory as Organizational Knowledge
**Full Title:** Institutional Memory as Organizational Knowledge: AI Agents That Learn Their Jobs from Experience, Not Instructions

Frames (S)AGE through the lens of organizational knowledge management — agents that accumulate and share institutional knowledge through governed experience rather than prompt engineering. Explores the implications for multi-agent system design and sovereign AI infrastructure.

## Priority & Provenance

All papers are timestamped via:
- **Git commit history** — initial commit [`23b4593`](https://github.com/l33tdawg/sage/commit/23b45930b0dc097f56978a99f45a11c93571b60b) dated 2026-03-02
- **Zenodo DOI** — permanent, citable digital object identifiers (see below)
- **GitHub Release** — tagged releases with SHA-256 checksums

## Zenodo DOIs

| Paper | DOI |
|-------|-----|
| Paper 1 | *Pending upload* |
| Paper 2 | *Pending upload* |
| Paper 3 | *Pending upload* |

DOIs will be updated here after Zenodo upload.

## SHA-256 Checksums

```
03718b72ce2a1db47befeffa6108a64ac65f9166b3a93cc01dc88258baada234  Paper1 - Agent Memory Infrastructure - Byzantine-Resilient Institutional Memory for Multi-Agent Systems.pdf
93a1a21090def7448a572eb87acf41a4a81f6501186d62320edeba02ace44213  Paper2 - Consensus-Validated Memory Improves Agent Performance on Complex Tasks.pdf
e8f16b4dcf9868467de17b9ea9f983e2a3128fed55d33d32bfa24133f9de2e6d  Paper3 - Institutional Memory as Organizational Knowledge - AI Agents That Learn Their Jobs from Experience Not Instructions.pdf
```

## License

These papers are released under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — you are free to share and adapt with attribution.

## Citation

If you reference this work, please cite the relevant paper(s) using the Zenodo DOI or the following format:

```bibtex
@misc{sage2026infrastructure,
  title={Agent Memory Infrastructure: Byzantine-Resilient Institutional Memory for Multi-Agent Systems},
  author={Kannabhiran, Dhillon Andrew},
  year={2026},
  note={Available at: https://github.com/l33tdawg/sage}
}

@misc{sage2026consensus,
  title={Consensus-Validated Memory Improves Agent Performance on Complex Tasks},
  author={Kannabhiran, Dhillon Andrew},
  year={2026},
  note={Available at: https://github.com/l33tdawg/sage}
}

@misc{sage2026institutional,
  title={Institutional Memory as Organizational Knowledge: AI Agents That Learn Their Jobs from Experience, Not Instructions},
  author={Kannabhiran, Dhillon Andrew},
  year={2026},
  note={Available at: https://github.com/l33tdawg/sage}
}
```
