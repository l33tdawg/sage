package monitoring_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type alertFile struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []struct {
			Alert string `yaml:"alert"`
			Expr  string `yaml:"expr"`
			For   string `yaml:"for"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

func TestSignerFenceHeldAlertUsesThePublishedAgeGauge(t *testing.T) {
	raw, err := os.ReadFile("alerts.yml")
	require.NoError(t, err)

	var alerts alertFile
	require.NoError(t, yaml.Unmarshal(raw, &alerts))

	for _, group := range alerts.Groups {
		for _, rule := range group.Rules {
			if rule.Alert != "SignerFenceHeld" {
				continue
			}
			require.Equal(t, "sage_nonce_fence_oldest_age_seconds > 300", rule.Expr)
			require.Equal(t, "1m", rule.For)
			return
		}
	}
	t.Fatal("SignerFenceHeld alert is missing")
}
