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

Source and paper provenance:
- **Source Git history** — root commit of the current published history, [`f2097605`](https://github.com/l33tdawg/sage/commit/f2097605e48f212512337c016c2cbcf0acdeeff9) dated 2026-03-02; this source date does not establish a paper's publication date
- **Zenodo DOI** — archived paper versions and their individual publication dates (see below)
- **GitHub Release** — tagged source releases with SHA-256 checksums

### Public experiment materials (2026-10-05)

Papers 2, 3 and 4 draw on the Level Up CTF experiment pipeline. The published
current `main` tree carries `integrations/levelup/experiment_protocol.py` and `sage_bridge.py`.
The pipeline scripts, organizational wiring, agent mesh, generated challenge
artifacts and run records are excluded from that tree for IP reasons (see the Level Up
Integration section of `.gitignore`). A fresh clone therefore cannot rerun
those experiments or independently re-derive their reported statistics.

Paper 3, page 18, describes the experiment harness and associated materials as
"fully open-source." That description is broader than the materials available
in the current `main` tree. Its two published integration files provide a
protocol/statistics component and a bridge; they do not constitute the complete
experiment pipeline. This repository-side note corrects the availability claim.

Earlier repository provenance text named `23b45930b0dc097f56978a99f45a11c93571b60b`
as the initial commit. That historical commit remains accessible on GitHub,
but it is not an ancestor of the current published `main` history. The current
history begins at `f2097605e48f212512337c016c2cbcf0acdeeff9`. Cite an exact source
revision and the individual Zenodo records for archived paper versions and
publication dates. A historical source identifier alone does not establish
that the complete experiment materials are publicly available.

The PDF files are unchanged by this disclosure. Their sizes and MD5 digests
match the files listed in the four Zenodo records as checked on 2026-10-05.
The SHA-256 values below identify the PDFs in this repository; Paper 4's earlier
README checksum was stale and has been corrected. Read the archived PDFs'
reproducibility statements together with this disclosure.

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
10653d6e19b9a83ec3533ec16b69211f44b25c195dd982d88d810bcc1be35539  Paper4 - Longitudinal Learning in Governed Multi-Agent Systems - How Institutional Memory Improves Agent Performance Over Time.pdf
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
