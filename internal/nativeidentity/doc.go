// This package uses the public macOS Security framework and kernel socket API.
// SecCodeCheckValidity validates the running code and its host, including the
// sealed components needed for identity; it is not a full bundle-resource scan.
// SecCodeCopySigningInformation follows successful validation, and dynamic
// validation is repeated after metadata extraction.
//
// LOCAL_PEERTOKEN supplies the socket peer task's current audit token, not a
// frozen connect-time credential. Verify captures it before validation and checks
// it again afterward. The Security lookup uses that complete token, including
// process generation, rather than accepting a PID or resolving an executable
// path. No check can promise the process remains alive after return. Inherited
// descriptors can survive an exec; the caller's explicit signing requirement
// must reject other code, and every new credential issuance needs a fresh check.
// The current process identity does not attest who wrote previously buffered
// bytes. Admission protocols must challenge the verified peer with fresh server
// randomness, require its response on that socket, and recheck the identity before
// issuing credentials; do not bind authority to an unproven initial public key.
// A bearer credential must not be treated as ongoing OS process authentication.
//
// Security calls disable network access, but synchronous framework IPC has no
// cancellation API or hard wall-clock guarantee. Callers must bound concurrent
// admission checks and discard results after their own request deadline.
//
// References:
//   - https://developer.apple.com/documentation/security/seccodecheckvalidity(_:_:_:)
//   - https://developer.apple.com/documentation/security/seccodecopyguestwithattributes(_:_:_:_:)
//   - https://developer.apple.com/documentation/technotes/tn3127-inside-code-signing-requirements/
//   - https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/uipc_usrreq.c
package nativeidentity
