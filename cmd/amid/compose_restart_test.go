package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestClusterComposeRestartsStoppedConsensusEngines(t *testing.T) {
	data, err := os.ReadFile("../../deploy/docker-compose.yml")
	require.NoError(t, err)
	var compose struct {
		Services map[string]struct {
			Restart string `yaml:"restart"`
		} `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal(data, &compose))
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("cometbft%d", i)
		t.Run(name, func(t *testing.T) {
			service, ok := compose.Services[name]
			require.True(t, ok)
			require.Equal(t, "unless-stopped", service.Restart)
		})
	}
}
