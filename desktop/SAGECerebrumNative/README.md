# SAGE CEREBRUM Native

This package is the macOS v12 product surface. It uses SwiftUI and AppKit; it
does not embed WebKit, HTML, or the browser CEREBRUM renderer.

Build and test:

```bash
env \
  SWIFTPM_HOME="$PWD/desktop/SAGECerebrumNative/.swiftpm-home" \
  CLANG_MODULE_CACHE_PATH="$PWD/desktop/SAGECerebrumNative/.clang-module-cache" \
  swift test --package-path desktop/SAGECerebrumNative --disable-sandbox

bash scripts/build-native-cerebrum-macos.sh
```

The app discovers a running beta daemon through
`~/.sage-v12-beta/run/shell-control.sock`. `SAGE_HOME` may select an explicit
test home. `SAGE_API_URL` is accepted only as an explicit loopback development
override with a scheme, host, and port and no credentials, query, or fragment.

Current implementation scope is recorded in
[`../../docs/v12-native-product-status.md`](../../docs/v12-native-product-status.md).
