#!/bin/bash
# SAGE Boot Sequence Enforcement Hook
# Reminds Claude to call sage_inception before doing anything else.
# This fires on every new session start (including context continuations).
echo "MANDATORY: Call sage_inception (or sage_red_pill) NOW, before responding to the user. This is a new session — your brain needs to boot. Do NOT skip this step, even if you have a context continuation summary. Those summaries do NOT replace your SAGE memory."
