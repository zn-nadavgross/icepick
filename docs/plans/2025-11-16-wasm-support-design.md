# WASM Support Design

**Date:** 2025-11-16
**Status:** Approved
**Target:** Cloudflare Workers with R2Catalog

## Overview

Enable icepick to compile and run on WebAssembly (wasm32-unknown-unknown) target, specifically for Cloudflare Workers environments using R2Catalog.

### Current State

- ✓ iceberg-rust dependency removed (Phase 4 complete)
- ✓ Core icepick code compiles for WASM
- ✗ Blocked on `Send + Sync` trait bounds in Catalog trait
- ✗ reqwest not properly configured for WASM

### Goals

1. Enable R2Catalog to work in Cloudflare Workers
2. Maintain full functionality on native platforms
3. Keep codebase unified (no separate WASM fork)
4. Follow Rust/WASM ecosystem best practices

### Success Criteria

- ✓ `cargo check --target wasm32-unknown-unknown` passes
- ✓ R2Catalog compiles and works in Cloudflare Workers
- ✓ All existing native platform tests still pass
- ✓ Examples compile for both native and WASM

### Out of Scope

- S3TablesCatalog on WASM (AWS SDK not WASM-compatible)
- Tokio/async_std support (not available in WASM)
- Multi-threading in WASM (fundamentally single-threaded)
- WASM runtime testing (future work)

## Architecture

### The Core Problem

WASM is single-threaded, so `Send + Sync` trait bounds are:
1. Meaningless (no threads to send between)
2. Incompatible with reqwest on WASM (uses `Rc<RefCell<>>` types which are `!Send`)

The Rust/WASM ecosystem solution: `#[async_trait(?Send)]` for WASM targets.

### Three-Level Solution

**Level 1: Trait Definition**

```rust
// src/catalog/catalog_trait.rs

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm"), async_trait(?Send))]
pub trait Catalog: Send + Sync {
    async fn create_namespace(...) -> Result<()>;
    async fn load_table(...) -> Result<Table>;
    // ... all other methods
}
```

**Why this works:**
- Native: `#[async_trait]` requires `Send` futures, `Send + Sync` bounds enforced
- WASM: `#[async_trait(?Send)]` removes `Send` requirement, bounds ignored

**Level 2: Trait Implementations**

```rust
// Native: enforce Send + Sync
#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl Catalog for RestCatalog { ... }

// WASM: no Send + Sync requirement
#[cfg(target_family = "wasm")]
#[async_trait(?Send)]
impl Catalog for RestCatalog { ... }
```

**Level 3: Dependency Configuration**

```toml
# Native: rustls-tls for HTTPS
[target.'cfg(not(target_family = "wasm"))'.dependencies]
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }

# WASM: uses browser fetch API
[target.'cfg(target_family = "wasm")'.dependencies]
reqwest = { version = "0.12", default-features = false, features = ["json"] }
```

### Design Decisions

**Why duplicate impl blocks instead of trait methods?**
- Single source of truth for the API (trait definition)
- Type safety enforced at compile time per platform
- ~10 lines of duplication per catalog (acceptable)
- Prevents accidental API divergence

**Why not use a feature flag?**
- Target detection is automatic and reliable
- No risk of user misconfiguration
- Follows ecosystem best practices

## Implementation Details

### Files to Modify

**1. Cargo.toml**
- Move reqwest to target-specific dependencies
- Native gets `rustls-tls` feature
- WASM gets no TLS (uses browser fetch)

**2. src/catalog/catalog_trait.rs**
- Add conditional `#[async_trait]` / `#[async_trait(?Send)]`
- Keep `Send + Sync` bounds (ignored on WASM)

**3. src/catalog/rest/catalog_impl.rs**
- Duplicate `impl Catalog for RestCatalog` block
- One with `#[async_trait]` for native
- One with `#[async_trait(?Send)]` for WASM

**4. src/catalog/r2.rs**
- Same pattern as RestCatalog
- Primary WASM target

**5. src/catalog/s3_tables.rs**
- Add `#![cfg(not(target_family = "wasm"))]` to entire module
- AWS SDK doesn't support WASM

**6. src/lib.rs**
- Conditionally export S3TablesCatalog
- Keep R2Catalog always exported

**7. src/catalog/rest/client.rs**
- Conditionally import S3Tables-specific utilities
- Fix unused import warnings on WASM

### Code Pattern

Applied to RestCatalog, R2Catalog, and (native-only) S3TablesCatalog:

```rust
#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl Catalog for MyCatalog {
    async fn create_namespace(...) -> Result<()> {
        // implementation
    }
    // ... all other methods
}

#[cfg(target_family = "wasm")]
#[async_trait(?Send)]
impl Catalog for MyCatalog {
    async fn create_namespace(...) -> Result<()> {
        // exact same implementation
    }
    // ... all other methods
}
```

**Total Impact:**
- 1 file: dependency config changes
- 5 files: trait bound changes
- ~30 lines of duplication total (acceptable for type safety)

## Error Handling

### No New Error Types Needed

Existing `Error` enum handles:
- Network errors (reqwest on both platforms)
- Serialization errors (serde on both platforms)
- Catalog errors (platform-agnostic)

### Edge Cases

**S3TablesCatalog on WASM:**
- Entire module wrapped with `#![cfg(not(target_family = "wasm"))]`
- Conditional re-export in `src/lib.rs`
- Compile error if user tries to use it on WASM (clear error message)

**Unused imports on WASM:**
- S3Tables-specific utilities conditionally imported
- Prevents unused import warnings

**Transaction API:**
- No changes needed
- Already platform-agnostic

**ParquetWriter:**
- No changes needed
- arrow/parquet support WASM

## Testing Strategy

### Native Tests (existing)
- All existing unit tests continue to pass
- All doctest examples continue to work
- `cargo test` unchanged

### WASM Compilation Tests
- `cargo check --target wasm32-unknown-unknown --no-default-features`
- `cargo clippy --target wasm32-unknown-unknown --no-default-features`
- Zero warnings on both targets

### WASM Runtime Tests (future work)
- Requires Cloudflare Workers test environment
- Would use `wasm-pack test` or similar
- Document as future enhancement

## Implementation Plan

### Phase 1: Configure Dependencies (5 min)
1. Move reqwest to target-specific dependencies in Cargo.toml
2. Verify uuid already has "js" feature
3. Run `cargo check` to ensure native still works

### Phase 2: Update Catalog Trait (5 min)
4. Update `src/catalog/catalog_trait.rs` with conditional async_trait
5. Run `cargo check` to verify trait compiles

### Phase 3: Update Catalog Implementations (20 min)
6. Duplicate `impl Catalog for RestCatalog` with conditional compilation
7. Duplicate `impl Catalog for R2Catalog` with conditional compilation
8. Wrap `src/catalog/s3_tables.rs` with `#![cfg(not(target_family = "wasm"))]`
9. Update `src/lib.rs` to conditionally export S3TablesCatalog
10. Fix unused import warnings in `src/catalog/rest/client.rs`

### Phase 4: Verification (10 min)
11. Run `cargo test` - all tests should pass on native
12. Run `cargo check --target wasm32-unknown-unknown --no-default-features`
13. Run `cargo clippy --target wasm32-unknown-unknown --no-default-features`
14. Fix any remaining warnings

### Phase 5: Documentation (5 min)
15. Update README with WASM support status
16. Add code comments explaining conditional compilation
17. Commit with message: "feat: add WASM support for R2Catalog"

**Estimated Total Time: 45 minutes**

### Rollback Plan

All changes are non-breaking for native platforms. WASM support is additive only. If issues arise, can revert without affecting existing users.

## Success Metrics

- ✓ Native: `cargo test` passes (no regressions)
- ✓ WASM: `cargo check --target wasm32-unknown-unknown` passes
- ✓ Zero clippy warnings on both targets
- ✓ S3TablesCatalog only available on native platforms
- ✓ R2Catalog available on both native and WASM

## References

- [Cloudflare Workers Rust Docs](https://developers.cloudflare.com/workers/languages/rust/)
- [async-trait ?Send pattern](https://github.com/rustwasm/wasm-bindgen/issues/2409)
- [reqwest WASM support](https://github.com/seanmonstar/reqwest)
- Phase 4 Implementation: docs/plans/2025-11-16-iceberg-rust-replacement-implementation.md
