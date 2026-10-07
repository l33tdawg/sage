package mcp

// Windows does not expose a portable directory-fsync operation through Go:
// FlushFileBuffers requires a writable file handle and cannot safely flush the
// read-only directory handle returned by os.Open. The identity file itself was
// synced before the same-volume atomic rename, so preserve durable claimant
// availability instead of turning every first Windows launch into a fail-closed
// persistence error.
func syncClaimantIdentityDirectory(string) error { return nil }
