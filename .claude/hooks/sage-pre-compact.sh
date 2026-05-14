#!/bin/bash
# SAGE PreCompact hook — fires right before Claude Code compacts the conversation.
# Compaction discards turn-level detail to make room. SAGE should crystallise
# what was learned this session BEFORE that detail vanishes.
echo "MANDATORY before compaction: Call sage_reflect with a concise summary of (dos, don'ts) from this session, then sage_remember for any durable facts you want to keep. Once the context compacts, the per-turn detail is gone — only what you've committed to SAGE will survive."
