# I Gave My AI Coding Partner a Memory — Here's What Actually Changed

**By Dhillon Andrew Kannabhiran**

---

*This is the final article in a four-part series: [the problem](https://medium.com/@dhillon.andrew/leonard-shelby-and-the-ai-memory-problem-what-building-a-multi-agent-ai-ctf-platform-taught-me-4f4c8fd6ab78) → [the architecture](https://medium.com/@dhillon.andrew/memories-are-all-we-are-i-made-what-i-think-the-road-to-agi-is-missing-b1821744dc59) → [the experiments](https://medium.com/@dhillon.andrew/i-built-memory-infrastructure-for-ai-agents-and-they-started-learning-on-their-own-81825f96f985) → the results.*

The first three articles were about the idea, the infrastructure, and the lab data behind (S)AGE.

This one is different.

This is what happened when I stopped experimenting and started *using* it — wiring my actual coding partner, Claude, into the system and working with it day to day. What actually improved, what didn't, and where the line is between genuine gain and wishful thinking.

## What I Was Already Working With

Let me set the baseline fairly, because I think a lot of AI memory discourse skips this part.

I've been vibe coding for quite a while now — starting out with Cursor then moving to Claude Code. Mainly because of Opus. It's the best model for coding right now. Reasoning through architecture, catching bugs, shipping fast. And Claude Code isn't memoryless out of the box. It has CLAUDE.md project files that get auto-loaded every conversation, and a built-in auto-memory system that can persist notes between sessions.

But starting each session was like working with a colleague who reads the project wiki but doesn't remember what you actually worked on together yesterday. The facts are there. The tech stack, the file structure, the project goals — all of that persists through CLAUDE.md, but what doesn't persist is the *working context*. What we tried, what failed, why we abandoned one approach for another, what broke at 3 AM and how we fixed it.

That's the gap I was living with. Not absence of memory — absence of *experience*.

## Wiring It Up

Having finished the experiments from the previous article and writing them up, I decided it was time to tackle building an MCP server for (S)AGE. The integration is straightforward — SAGE exposes memory operations as MCP tools and Claude or any other agentic system that supports MCP can call them. Store a memory, recall by topic, reflect on what worked.

The first call was `sage_inception`. It checks if the AI already has a brain. Claude didn’t — fresh start. It needed seeding with foundational memories so I asked it to store its claude.md and any other ‘memory’ files it had on disk. It came back with 35 memories across 8 domains. Mostly meta-knowledge about how the system itself works.

I also needed a way for Claude to store observations on every conversation turn — a mechanism that triggers semantic recall, which I embedded into the MCP server instructions, and added a nudge system that reminds it if it forgets. So this isn’t the AI spontaneously deciding to remember things just because it has access to (S)AGE. (S)AGE is infrastructure — you still need to wire up that default behavior.

(S)AGE won’t magically make your AI remember. It’s the MCP integration that helps it do so and (S)AGE provides the infrastructure that makes remembering the path of least resistance.

## This Isn’t Just Retrieval

Before I get into the day-to-day experience, I want to make one thing clear — because it’s the part most people miss.

(S)AGE is not a RAG system. It’s not a vector database with a pretty wrapper.

When Claude stores an observation, it doesn’t just land in a database. It goes through four application validators that independently check for duplicates, quality, consistency, and sanity. Only when three out of four agree — a BFT quorum — does the memory get committed. Memories that aren’t corroborated over time decay. Memories that are corroborated get stronger. The knowledge base self-corrects.

This isn’t something I bolted on later. Consensus-backed governance *is* the system — it’s what the second article in this series was about. And it’s the reason the memories Claude retrieves are useful instead of noisy.

## So What Actually Changed

**The re-explaining got shorter, not eliminated.**

Before SAGE, I'd spend the first several minutes of each session pointing Claude at the right context — "check the plan," "look at what we did yesterday," "read the memory file." With SAGE, the `sage_turn` call at the start of each conversation pulls back relevant memories based on the current topic. So when I say "the RBAC thing is broken," Claude already has context about the RBAC subsystem from previous sessions — the architecture, what we changed, what broke before, and what we fixed. 

But it's not perfect. 

Sometimes the semantic recall pulls back irrelevant memories. Sometimes important context doesn't surface because it was stored under a different domain tag. Sometimes Claude still needs to grep through files to understand the current state of the code. SAGE gives it a head start, not *omniscience*.

**Cross-task connections happened more naturally.**

This was the most surprising improvement. On that first day, we wrote the (S)AGE GUI (previously Lite), built a landing page, fixed security issues, set up GoReleaser, built installers for macOS and Windows, created browser extensions, wrote a privacy policy, and designed a user facing brain dashboard we decided to call CEREBRUM. 

The interesting part wasn't the volume of work done — it was how knowledge carried between tasks. When writing the landing page copy, Claude drew on the security review we'd done earlier to inform the trust messaging. When we designed the browser extension, architecture decisions from the dashboard work carried over. When we discussed the privacy policy, Claude already had context about the encryption-at-rest conversation.

Would some of that have happened anyway, within a single long context window? Probably. But SAGE stores observations as they happen and recalls them by relevance, which means connections surface even across session boundaries — not just within a single conversation.

**The "you know what I'm talking about" effect was real.**

This turned out to be the single most important change. It's the most consistent improvement, and the hardest to replicate with flat files.

When you work with someone for two weeks, you develop a shared shorthand. "The RBAC thing." "That bug from Tuesday." "The approach we abandoned." A colleague who's been in the trenches with you knows what these refer to. A new contractor doesn't.

SAGE doesn't give Claude perfect recall of everything we've ever discussed. But it gives it enough contextual memory that shorthand works. I can say "the cross-agent visibility issue" and Claude surfaces the relevant memories — the org-based visibility fix, the four bugs the LevelUp pipeline reported, the v4.5 patches. It doesn't need the full briefing. It has enough institutional memory to fill in the gaps.

That's not magic. It's a semantic search over a governed knowledge base. But — and this is the part that matters — the reason that search returns useful results instead of noise is because everything in there went through a quality gate before it was committed. The shorthand works because the memories backing it are trustworthy, not just plentiful.

The practical effect is that I spend less time explaining and more time building. And that compounds.

**The relationship shifted — subtly.**

I don't want to overstate this because it sounds like the kind of thing people say to make AI sound more impressive than it is. But I did notice a shift from "tool I use" to something slightly closer to "colleague I work with."

The difference is small but real: I started saying "remember when we..." and it did. I started trusting it with more context because I knew the context would persist. I stopped over-specifying tasks because the institutional knowledge filled in what the prompt left out.

Before SAGE, Claude knew *about* the project from CLAUDE.md. After SAGE, it knew what we'd been *through* together. Those are different things, and they produce different working dynamics.

## What Didn't Change

I want to be equally honest about what SAGE didn't improve.

**Code quality stayed the same.** Claude's code is a function of the underlying model, not its memory. SAGE doesn't make it write better Go or catch more bugs. It writes the same quality code — it just has better context about *what* code to write.

**Hallucinations still happen.** Memory doesn't prevent confabulation. Claude can still confidently state things that aren't true. The consensus layer helps — if a bogus observation somehow makes it past the validators, it'll decay over time without corroboration. But in the moment, the AI can still be wrong about things it "remembers." Governance reduces the odds of bad knowledge persisting. It doesn't eliminate bad knowledge from being proposed in the first place.

**It's not a replacement for reading the code.** SAGE gives Claude context about what we've worked on, but the actual state of the codebase is in the codebase. It still needs to read files, grep for patterns, check git logs. Memory supplements code exploration — it doesn't replace it.

**The AI still needs to be told to use it.** I mentioned this already, but it bears repeating. Claude calls SAGE because the instructions say to. Remove the MCP server instructions and the CLAUDE.md boot sequence, and it goes right back to being stateless. The memory behavior is scaffolded, not intrinsic.

## What I Think This Actually Means

SAGE isn't a silver bullet. It doesn't turn a good AI into a great one. It doesn't eliminate the fundamental limitations of working with language models.

But using it day-to-day is different from running controlled experiments. The lab showed me the numbers. Living with it showed me the *feel*.

Over the first week of using it, we shipped a lot — RBAC, native launchers, a Python SDK, security audits, the CEREBRUM dashboard. I was working intensely — sleeping too little, running on momentum. SAGE didn't do that work. But it reduced the friction that normally slows down intense building sprints. Every session picked up with context. Every lesson stayed in the knowledge base. Every bug we fixed informed the next fix.

By the end of the week, Claude had over 700 committed memories across 40+ domains — not raw observations dumped into a vector store, but memories that survived the consensus pipeline. That's what governance buys you in practice. Not more memories — *better* ones.

In the first article I laid out six things that were missing: persistence, learning, validation, governance, cross-agent access, accumulation. Most "memory for AI" solutions stop at the first two — they give Leonard Shelby a better filing cabinet. (S)AGE addresses all six. The filing cabinet matters, but so does making sure what goes *into* it is actually worth keeping.

I started this series with a twelve-agent pipeline where knowledge evaporated between sessions and difficulty calibration bounced like a pinball. I built infrastructure to fix that — not a wrapper, not a library, but consensus-backed governed memory. The experiments proved it worked in the lab. Living with it proved it works where it matters — in the messy, unpredictable, 3 AM debugging sessions where you just need your AI to *remember what we already tried*.

Is it worth the overhead? For a weekend hobby project, probably not. For anyone doing sustained, multi-session AI collaboration on a complex codebase — I think the answer is obviously yes, and the gap only widens as the memory accumulates.

## Try It Yourself

(S)AGE is open source under Apache 2.0. The personal edition runs locally — single binary, no Docker, no cloud. Your memories stay on your hardware, encrypted behind AES-256.

**GitHub:** [github.com/l33tdawg/sage](https://github.com/l33tdawg/sage)

The full series of white papers, the codebase, and everything I've described across these four articles is free. Because — as always — we don't gatekeep stuff.

Install it, wire it up, and see what happens when your AI stops forgetting everything you did yesterday.

No more tattoos.

---

*Dhillon Andrew Kannabhiran is the creator of (S)AGE and LevelUp CTF. He builds at the intersection of security, consensus systems, and AI infrastructure. Previously: founder of Hack In The Box (HITB), one of Asia's longest-running technical security conferences.*

*In memory of Felix 'FX' Lindner — who showed us how much further curiosity can go.*
