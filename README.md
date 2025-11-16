# Rust Iceberg + AWS S3 Tables PoC

Proof-of-concept to validate rust-iceberg compatibility with AWS S3 Tables REST API.

## ⚠️ POC Result: Incompatible

**Finding:** rust-iceberg (v0.7.0) **cannot** currently work with AWS S3 Tables due to missing AWS SigV4 authentication support in the REST catalog client.

See [Findings](#findings) below for details.

## Prerequisites

1. AWS credentials configured (via `~/.aws/credentials` or environment variables)
2. S3 Tables bucket created via AWS console
3. IAM permissions for `s3tables:*` operations

## Usage

```bash
cargo run -- \
  arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket \
  my_namespace \
  hello_table
```

## What it does

1. Parses S3 Tables ARN and extracts region
2. Connects to S3 Tables REST catalog with SigV4 signing
3. Creates namespace (if doesn't exist)
4. Creates table with simple schema: `{ id: i64 }`
5. Writes 3 rows: [1, 2, 3]
6. Reads data back
7. Prints both datasets for visual verification

## Actual Output

```
✓ Connected to S3 Tables catalog
Error: Failed to create namespace

Caused by:
    Unexpected, context: { status: 403 Forbidden, headers: {..., "x-amzn-errortype": "MissingAuthenticationTokenException", ...}, json: {"message":"Missing Authentication Token"} }
```

The catalog connection succeeds, but all subsequent operations fail with `403 Forbidden` because requests are not signed with AWS SigV4.

## Findings

### Why It Doesn't Work

**Root Cause:** AWS S3 Tables requires AWS SigV4 signing on all REST API requests, but rust-iceberg's REST catalog client does not support request signing.

**Technical Details:**

1. **S3 Tables Requirement**
   - Endpoint: `https://s3tables.{region}.amazonaws.com/iceberg`
   - Authentication: AWS SigV4 with service name `s3tables`
   - All requests must be signed (catalog operations, data I/O, etc.)

2. **rust-iceberg Limitation**
   - `RestCatalogBuilder.with_client()` accepts only `reqwest::Client`
   - No built-in SigV4 support (unlike Java's `RESTSigV4Signer`)
   - No request interceptor/middleware hooks
   - Cannot use `reqwest-middleware::ClientWithMiddleware` (type mismatch)

3. **Why Custom Signing Failed**
   - `reqwest::Client` has no interceptor hooks for signing requests
   - `reqwest-middleware` solves this but returns incompatible type
   - `RestCatalogBuilder` doesn't accept middleware-wrapped clients

### What Would Be Needed

To make rust-iceberg work with S3 Tables, one of these changes is required:

**Option 1:** Add built-in SigV4 support to iceberg-catalog-rest
- Implement `AwsV4Signer` integration (using `reqsign` crate)
- Add catalog config properties: `rest.sigv4-enabled`, `rest.signing-name`, `rest.signing-region`
- Similar to Java implementation's `RESTSigV4Signer`

**Option 2:** Support custom middleware clients
- Change `with_client()` to accept `impl Into<ClientWithMiddleware>`
- Allow users to provide pre-configured signed clients
- Most flexible for different auth methods

**Option 3:** Add request interceptor hooks
- Add callback/trait for request modification before sending
- Allow users to inject custom signing logic
- More generic than Option 1

### Current Workarounds

**Use PyIceberg instead:**
- Python's iceberg library supports S3 Tables with SigV4
- Example: `Catalog.from_s3tables("arn:aws:s3tables:...")`
- Confirmed working (see Daft integration)

**Use Java Iceberg:**
- `RESTSigV4Signer` provides full S3 Tables support
- Production-ready implementation

## Known S3 Tables Limitations

Beyond the authentication issue, S3 Tables has these documented limitations:
- Limited schema evolution
- No time travel/snapshots via REST
- Partition evolution restrictions
- Nested types may have limited support

This PoC was designed to avoid these limitations by using a minimal schema.

## Development Workflow

### Git Hooks + Tooling

All Git hooks are managed through [`pre-commit`](https://pre-commit.com/) so you never have to install Python packages globally. The repo assumes you are using [Astral's `uv`](https://github.com/astral-sh/uv); run the installer via `uvx` (or the shorter `ux` alias if you have it configured) and let it bootstrap both the `pre-commit` binary and the hook environments:

```bash
# Install the pre-commit client + hook wrappers
uvx pre-commit install --hook-type pre-commit --hook-type pre-push --hook-type commit-msg

# Optional: run everything against the whole tree
uvx pre-commit run --all-files
```

The configured hooks cover these checks:

- Source hygiene: whitespace cleanup, EOF newlines, and preventing 5MB+ blobs
- `cargo fmt`, `cargo clippy --locked --all-targets --all-features -D warnings`, and `cargo test --locked --all-features` (tests run on `pre-push`)
- `scripts/enforce_quality.py` which enforces a per-file 400 LOC and complexity score of 60 by looking at control-flow keywords; tune via `MAX_LOC_PER_FILE` / `MAX_COMPLEXITY_SCORE`
- Conventional commit messages through `compilerla/conventional-pre-commit`

You can run the quality script directly if you want a quick readout:

```bash
python scripts/enforce_quality.py
# or specify tighter limits
python scripts/enforce_quality.py --max-loc 250 --max-complexity 45
```

These defaults should keep this small PoC tidy; adjust the thresholds as the codebase grows, but keep the hook steps in CI to prevent regressions.
