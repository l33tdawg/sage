package main

import (
	"context"
	"errors"
	"math"
	"strconv"

	"github.com/l33tdawg/sage/internal/store"
	"gopkg.in/yaml.v3"
)

type PrivateMediaConfig struct {
	Enabled      bool  `yaml:"enabled"`
	ActorBytes   int64 `yaml:"actor_bytes"`
	NodeBytes    int64 `yaml:"node_bytes"`
	ActorObjects int64 `yaml:"actor_objects"`
	NodeObjects  int64 `yaml:"node_objects"`
	MinFreeBytes int64 `yaml:"min_free_bytes"`
}

func (config *PrivateMediaConfig) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode || len(node.Content)%2 != 0 {
		return errors.New("private_media requires a mapping")
	}
	var parsed PrivateMediaConfig
	numbers := map[string]*int64{"actor_bytes": &parsed.ActorBytes, "node_bytes": &parsed.NodeBytes,
		"actor_objects": &parsed.ActorObjects, "node_objects": &parsed.NodeObjects, "min_free_bytes": &parsed.MinFreeBytes}
	seen := make(map[string]bool)
	for index := 0; index < len(node.Content); index += 2 {
		key, value := node.Content[index], node.Content[index+1]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || seen[key.Value] || value.Kind != yaml.ScalarNode {
			return errors.New("invalid private_media field")
		}
		seen[key.Value] = true
		if key.Value == "enabled" {
			if value.Tag != "!!bool" || (value.Value != "true" && value.Value != "false") {
				return errors.New("private_media.enabled requires true or false")
			}
			parsed.Enabled = value.Value == "true"
			continue
		}
		target, present := numbers[key.Value]
		if !present || value.Tag != "!!int" {
			return errors.New("private_media quotas require known integer fields")
		}
		number, err := strconv.ParseInt(value.Value, 10, 64)
		if err != nil || number < 0 || strconv.FormatInt(number, 10) != value.Value {
			return errors.New("private_media quotas require nonnegative decimal integers")
		}
		*target = number
	}
	if err := parsed.validate(); err != nil {
		return err
	}
	*config = parsed
	return nil
}

func (config PrivateMediaConfig) validate() error {
	if config.ActorBytes < 0 || config.NodeBytes < 0 || config.ActorObjects < 0 || config.NodeObjects < 0 || config.MinFreeBytes < 0 {
		return errors.New("private_media quotas must be nonnegative")
	}
	if !config.Enabled {
		return nil
	}
	if config.ActorBytes == 0 || config.NodeBytes < config.ActorBytes || config.ActorObjects == 0 ||
		config.NodeObjects < config.ActorObjects || config.MinFreeBytes == 0 ||
		config.MinFreeBytes > math.MaxInt64-4*(store.MaxPrivateJPEGBytes+1024)-65536 {
		return errors.New("enabled private_media requires positive explicit ordered quotas and overflow-safe free-space reserve")
	}
	return nil
}

type privateMediaProbeFactory func(string) (store.PrivateMediaSpaceProbe, func(), error)

func configurePrivateMediaStore(ctx context.Context, config PrivateMediaConfig, sqlite *store.SQLiteStore, path string, factory privateMediaProbeFactory) (*store.PrivateMediaStore, func(), error) {
	noop := func() {}
	if err := config.validate(); err != nil {
		return nil, noop, err
	}
	if !config.Enabled {
		return nil, noop, nil
	}
	if factory == nil || sqlite == nil {
		return nil, noop, errors.New("private media backend unavailable")
	}
	probe, closeProbe, err := factory(path)
	if err != nil {
		if closeProbe != nil {
			closeProbe()
		}
		return nil, noop, err
	}
	if closeProbe == nil || probe == nil {
		if closeProbe != nil {
			closeProbe()
		}
		return nil, noop, errors.New("private media capacity probe unavailable")
	}
	available, err := probe(ctx)
	if err != nil || available <= 0 {
		closeProbe()
		return nil, noop, errors.New("private media capacity probe failed")
	}
	media, err := store.NewPrivateMediaStore(sqlite, store.PrivateMediaQuota{Enabled: true, ActorBytes: config.ActorBytes,
		NodeBytes: config.NodeBytes, ActorObjects: config.ActorObjects, NodeObjects: config.NodeObjects,
		MinFreeBytes: config.MinFreeBytes}, probe)
	if err != nil {
		closeProbe()
		return nil, noop, err
	}
	return media, closeProbe, nil
}
