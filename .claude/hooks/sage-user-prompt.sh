#!/bin/bash
# SAGE UserPromptSubmit hook — fires when the user submits a new prompt.
# Soft nudge so the agent calls sage_turn early in its response, capturing the
# new conversational state into episodic memory without depending on the agent
# to remember on its own.
echo "Reminder: call sage_turn early in your response with the topic + an observation of what just happened. Memories you don't store don't survive."
