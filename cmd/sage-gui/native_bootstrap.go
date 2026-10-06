//go:build !nativebootstraptestfixture

package main

import "github.com/l33tdawg/sage/internal/nativeidentity"

// The beta app has its own identifier and must be a hardened Developer ID
// application from SAGE's signing team. Unsupported/no-cgo builds fail closed
// in the peer verifier; they do not fall back to UID-only admission.
func nativeBootstrapRequirement() string {
	if !nativeidentity.Supported() {
		return ""
	}
	return `anchor apple generic and identifier "com.sage.cerebrum.beta" and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = "2N7GKZ8D8Z"`
}
