package mcp

import "testing"

// sage_find_agent could only ever match a name EXACTLY. matchesAgentName
// returned a `partial` result that it never set to true, so `localPartial` in
// toolFindAgent and its fallback were dead code — the intent was there and the
// implementation was not.
//
// That made any descriptive name unaddressable. An agent registered as
// "MYNAH (SAGE Voice Bridge Agent)" — which is what CEREBRUM should show, so an
// operator granting domain access knows which row is which — could not be
// reached by "send this to mynah", which is what a human actually says.
//
// The safety argument for allowing partials at all: exact matches rank first
// across local and federated results, and every match is
// returned with its agent_id for the model to choose from rather than resolved
// silently to one.
func TestMatchesAgentName(t *testing.T) {
	const display = "MYNAH (SAGE Voice Bridge Agent)"

	cases := []struct {
		name           string
		query          string
		candidates     []string
		exact, partial bool
	}{
		// The phrases the owner actually uses.
		{"the brand alone", "mynah", []string{display}, false, true},
		{"what it does", "voice bridge", []string{display}, false, true},
		{"how they refer to it", "the voice notes agent", []string{display, "voice notes"}, false, true},
		{"spoken with filler", "the mynah agent", []string{display}, false, true},

		// Exact still wins, and still means exact.
		{"exact wins", display, []string{display}, true, false},
		{"case is not identity", "mynah (sage voice bridge agent)", []string{display}, true, false},
		{"punctuation is display, not identity", "mynah sage voice bridge agent", []string{display}, true, false},

		// The immutable registered name is checked alongside the display name,
		// which is what lets an agent be renamed without becoming unreachable.
		{"registered name still matches after a rename", "sage voice bridge",
			[]string{"MYNAH Voice Bridge", "SAGE Voice Bridge"}, true, false},

		// Wrong agent is worse than no agent: sage_find_agent feeds sage_pipe.
		{"a different agent", "macbook pro agent a", []string{display}, false, false},
		{"one weak shared word does not match", "agent", []string{display}, false, false},
		{"unrelated", "perplexity", []string{display}, false, false},
		{"filler only", "the", []string{display}, false, false},
		{"empty query", "", []string{display}, false, false},
		{"empty candidate", "mynah", []string{""}, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exact, partial := matchesAgentName(tc.query, tc.candidates...)
			if exact != tc.exact || partial != tc.partial {
				t.Fatalf("matchesAgentName(%q, %v) = (exact=%v, partial=%v), want (exact=%v, partial=%v)",
					tc.query, tc.candidates, exact, partial, tc.exact, tc.partial)
			}
		})
	}
}

// Two agents both branded MYNAH must stay distinguishable, or "send this to the
// voice bridge" is a coin flip between the phone appliance and the Mac app.
func TestTheTwoMynahAgentsStayDistinguishable(t *testing.T) {
	const appliance = "MYNAH (SAGE Voice Bridge Agent)"
	const app = "MYNAH (Mac App)"

	if _, partial := matchesAgentName("voice bridge", app); partial {
		t.Fatal("\"voice bridge\" matched the Mac app")
	}
	if _, partial := matchesAgentName("mac app", appliance); partial {
		t.Fatal("\"mac app\" matched the appliance")
	}
	// The shared brand matches both on purpose — that is a disambiguation
	// prompt, not a wrong answer, and toolFindAgent returns both with their ids.
	for _, candidate := range []string{appliance, app} {
		if _, partial := matchesAgentName("mynah", candidate); !partial {
			t.Fatalf("%q was unreachable by the brand name", candidate)
		}
	}
}
