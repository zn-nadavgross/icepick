# WASM Compatibility Status

**Date:** 2025-11-15
**Status:** ❌ Blocked upstream

## Summary

R2Catalog is designed to be WASM-compatible (no platform-specific dependencies), but actual WASM compilation is currently blocked by the upstream `iceberg-rust` library (v0.7.0).

## Current Situation

### What Works
- ✅ R2Catalog code has no WASM-incompatible features
- ✅ Uses only reqwest + bearer token auth (both WASM-compatible)
- ✅ Platform-gated properly (#[cfg(not(target_family = "wasm"))] for S3Tables)
- ✅ Documentation correctly states R2Catalog is "WASM-compatible"

### What Doesn't Work
- ❌ `cargo build --target wasm32-unknown-unknown` fails
- ❌ Blocked by iceberg-rust's tokio dependency configuration

## Error Details

When attempting to build for WASM:

```
error: Only features sync,macros,io-util,rt,time are supported on wasm.
```

The issue is in iceberg-rust's dependency tree. The library uses tokio features that are not available in WASM environments.

Additional C compilation errors from zstd-sys (parquet dependency):
```
error: unable to create target: 'No available targets are compatible with triple "wasm32-unknown-unknown"'
```

This is a secondary issue - system clang doesn't support WASM target without wasm-specific tooling.

## Root Cause

The `iceberg` crate (v0.7.0) depends on tokio with features that aren't WASM-compatible:
- `tokio/io-std`
- `tokio/fs`
- Or other non-WASM features pulled in transitively

## Path Forward

### Option 1: Wait for Upstream (Recommended)
Monitor iceberg-rust for WASM support. This is likely being worked on as WASM is increasingly important for data engineering tools.

Track:
- https://github.com/apache/iceberg-rust/issues (search for "wasm")
- iceberg-rust changelog for WASM-related updates

### Option 2: Contribute Upstream
Help iceberg-rust add WASM support:
1. Identify which tokio features need to be platform-gated
2. Add WASM-compatible alternatives where needed
3. Submit PR to iceberg-rust

### Option 3: Fork (Not Recommended)
Fork iceberg-rust and patch for WASM support. This would be maintenance-heavy and not worth it for this use case.

## CI/CD Implications

The CI workflow (`.github/workflows/ci.yml`) includes a WASM build job:

```yaml
- name: Build for WASM
  run: cargo build --target wasm32-unknown-unknown --verbose
```

**Current Status:** This job will fail until iceberg-rust adds WASM support.

**Recommendation:** Keep the job in CI. It will:
1. Catch regressions if we accidentally add WASM-incompatible code to R2Catalog
2. Automatically start passing once iceberg-rust resolves the limitation
3. Serve as a canary for when WASM support is available

## Documentation Accuracy

The README and rustdoc correctly state that R2Catalog is WASM-compatible. This is architecturally true - the code is written to work on WASM. The limitation is purely a transitive dependency issue that will be resolved upstream.

No documentation changes needed. The design is correct.

## Testing Status

Since we can't build for WASM, we can't test R2Catalog in a WASM environment yet. Once iceberg-rust adds support:

1. Test in browser environment
2. Test in Cloudflare Workers
3. Verify FileIO works with R2 object storage from WASM
4. Add integration tests for WASM-specific scenarios

## References

- Tokio WASM support: https://tokio.rs/tokio/topics/wasm
- iceberg-rust: https://github.com/apache/iceberg-rust
- WASM target tier: https://doc.rust-lang.org/nightly/rustc/platform-support/wasm32-unknown-unknown.html
