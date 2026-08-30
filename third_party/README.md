# Third-party license inputs

EpochDeck's release notice generator normally reads license files packaged with
locked Cargo and pnpm dependencies. Some upstream packages declare a license
but omit its text from their published archive. The version-specific files in
`license-overrides/` close that gap without making notice generation depend on
the network.

Every override in `license-overrides.json` records the exact dependency
identity and license expression, the provenance of the fallback text, and the
SHA-256 of the checked-in bytes. The generator fails if an override is stale,
unused, modified, or no longer matches dependency metadata. Prefer upstream
release/tag content. A synthesized text is allowed only when upstream publishes
the license declaration but no license file; record that fact in `provenance`.

Static Linux releases also link code that is not represented by Cargo metadata.
`release-toolchain.json` pins the Rust, LLVM, musl, GCC runtime, Zig, and
cargo-zigbuild inputs used by the release workflow. Its components explain
which runtime bytes may be linked and map each component to immutable,
hash-checked license documents under `toolchain-runtime/`. The generator checks
the manifest against the exact workflow versions and musl targets, and fails on
missing pins, components, metadata, text, or a changed document hash.

The dashboard inventory includes locked production packages plus an explicit
bounded set of build tools that can emit production bytes: Vite, Rollup, and
esbuild. The `svelte` production package covers both its runtime and compiler
output. `@sveltejs/vite-plugin-svelte` only orchestrates the production
transform and does not contribute a separately shipped runtime. When the build
pipeline changes, re-audit that classification instead of silently expanding
the build dependency closure. The notice fingerprints `web/vite.config.ts` and
`web/svelte.config.js` so production compiler-pipeline configuration changes
cannot bypass that review.

These files cover third-party dependencies only. They do not select or declare
EpochDeck's own project license.
