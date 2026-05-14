#!/bin/bash
# SAGE Stop hook — fires when the agent finishes a response.
# Used as a soft reminder that the agent should keep its episodic memory
# up to date by calling sage_turn at least once per turn, and sage_reflect
# at session boundaries. The hook itself doesn't write to SAGE — that's
# the agent's job — it just nudges if the recent turn appears to lack
# memory activity.
#
# Exit silently so it doesn't add visible chatter on every assistant turn.
exit 0
