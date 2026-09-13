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

### Paper 4: Longitudinal Learning in Governed Multi-Agent Systems
**Full Title:** Longitudinal Learning in Governed Multi-Agent Systems: How Institutional Memory Improves Agent Performance Over Time

Presents a longitudinal study measuring whether consensus-validated institutional memory produces measurable, cumulative improvement in AI agent performance across sequential task executions. Using a 4-node BFT-backed memory layer and an 11-agent organization with 3-line prompts and zero domain expertise, conducts three experiments: a difficulty sweep, 10 sequential runs with red team feedback, and a 20-run control arm. Demonstrates statistically significant learning trends (Spearman rho=0.716, p=0.020) in the SAGE arm with no learning in the control (rho=0.040, p=0.901).

## Priority & Provenance

All papers are timestamped via:
- **Git commit history** — root commit of the published history, [`f2097605`](https://github.com/l33tdawg/sage/commit/f2097605e48f212512337c016c2cbcf0acdeeff9) dated 2026-03-02
- **Zenodo DOI** — permanent, citable digital object identifiers (see below)
- **GitHub Release** — tagged releases with SHA-256 checksums

Reproducibility note: Papers 2 and 4 draw on the Level Up CTF experiment pipeline. The
published tree carries `integrations/levelup/experiment_protocol.py` and `sage_bridge.py`;
the pipeline's scripts and run records are excluded for IP reasons (see the Level Up
Integration section of `.gitignore`), so the paper statistics cannot be re-derived from a
fresh clone. The PDFs and the Zenodo records above are the citable artifacts.

## Zenodo DOIs

| Paper | DOI | Zenodo Record |
|-------|-----|---------------|
| Paper 1 | [10.5281/zenodo.18856658](https://doi.org/10.5281/zenodo.18856658) | [zenodo.org/records/18856658](https://zenodo.org/records/18856658) |
| Paper 2 | [10.5281/zenodo.18856774](https://doi.org/10.5281/zenodo.18856774) | [zenodo.org/records/18856774](https://zenodo.org/records/18856774) |
| Paper 3 | [10.5281/zenodo.18856845](https://doi.org/10.5281/zenodo.18856845) | [zenodo.org/records/18856845](https://zenodo.org/records/18856845) |
| Paper 4 | [10.5281/zenodo.18888597](https://doi.org/10.5281/zenodo.18888597) | [zenodo.org/records/18888597](https://zenodo.org/records/18888597) |
| Code (v1.0.1) | [10.5281/zenodo.18855836](https://doi.org/10.5281/zenodo.18855836) | [zenodo.org/records/18855836](https://zenodo.org/records/18855836) |

## SHA-256 Checksums

```
c10859717df7cde0d986f43526931df0ac6667964d0347195d9388b8e9fcbc72  Paper1 - Agent Memory Infrastructure - Byzantine-Resilient Institutional Memory for Multi-Agent Systems.pdf
277c4a3ad290c4d645da5e00a9596bf9bc3ec34f67ffa747580dadfa8b592796  Paper2 - Consensus-Validated Memory Improves Agent Performance on Complex Tasks.pdf
e8f16b4dcf9868467de17b9ea9f983e2a3128fed55d33d32bfa24133f9de2e6d  Paper3 - Institutional Memory as Organizational Knowledge - AI Agents That Learn Their Jobs from Experience Not Instructions.pdf
b2c1e0e87f00da74832480130f432230f2cde42e55869b15c20cf8909c8bb767  Paper4 - Longitudinal Learning in Governed Multi-Agent Systems - How Institutional Memory Improves Agent Performance Over Time.pdf
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

@misc{sage2026longitudinal,
  title={Longitudinal Learning in Governed Multi-Agent Systems: How Institutional Memory Improves Agent Performance Over Time},
  author={Kannabhiran, Dhillon Andrew},
  year={2026},
  note={Available at: https://github.com/l33tdawg/sage}
}
```
