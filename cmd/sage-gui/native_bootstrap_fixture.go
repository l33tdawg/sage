//go:build nativebootstraptestfixture

package main

// This symbol only exists in disposable qualification binaries. There is no
// environment or runtime configuration override in an ordinary daemon build.
var nativeBootstrapFixtureRequirement string

func nativeBootstrapRequirement() string { return nativeBootstrapFixtureRequirement }
