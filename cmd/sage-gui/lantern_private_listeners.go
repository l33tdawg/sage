package main

import (
	"errors"
	"os"

	"gopkg.in/yaml.v3"
)

func validateLanternPrivateYAML(data []byte) error {
	if os.Getenv("SAGE_LANTERN_PRIVATE_LISTENERS") == "" {
		return nil
	}
	var explicit struct {
		Federation *struct {
			Enabled *bool `yaml:"enabled"`
		} `yaml:"federation"`
		Quorum *struct {
			Enabled *bool `yaml:"enabled"`
		} `yaml:"quorum"`
	}
	if err := yaml.Unmarshal(data, &explicit); err != nil {
		return errors.New("invalid Lantern private configuration")
	}
	if explicit.Federation == nil || explicit.Federation.Enabled == nil || *explicit.Federation.Enabled ||
		explicit.Quorum == nil || explicit.Quorum.Enabled == nil || *explicit.Quorum.Enabled {
		return errors.New("Lantern private node requires explicit federation.enabled=false and quorum.enabled=false")
	}
	return nil
}

func enforceLanternPrivateListeners(cfg *Config) error {
	policy := os.Getenv("SAGE_LANTERN_PRIVATE_LISTENERS")
	if policy == "" {
		return nil
	}
	if policy != "1" {
		return errors.New("invalid Lantern private listener policy")
	}
	if cfg.Federation.Enabled || cfg.Quorum.Enabled {
		return errors.New("Lantern private node forbids federation and quorum")
	}
	if cfg.RESTAddr != "127.0.0.1:8080" || cfg.Quorum.TLSAddr != "127.0.0.1:8443" ||
		cmtRPCAddr() != "tcp://127.0.0.1:26657" || cmtP2PAddr("tcp://127.0.0.1:26656") != "tcp://127.0.0.1:26656" {
		return errors.New("Lantern private node requires fixed loopback listeners")
	}
	return nil
}
