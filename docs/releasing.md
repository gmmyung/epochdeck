# Releasing EpochDeck

EpochDeck releases are explicit GitHub prereleases. The workflow attaches
native server archives, a Python wheel, a source distribution, and
`SHA256SUMS`. It does not publish to PyPI, crates.io, or npm.

The authoritative build matrix lives in
[`prerelease.yml`](../.github/workflows/prerelease.yml). Each native job tests its
target, inspects the binary, and smoke-tests the finished archive.

## Third-party notices

After changing `Cargo.lock`, `web/package.json`, `web/pnpm-lock.yaml`, the
dashboard build pipeline, or the static release toolchain, install the locked
dependencies and regenerate the audited notice:

```bash
just bootstrap
just third-party-notices-generate
```

Review both the dependency-index change and every newly required license text.
Packages that omit license files need an exact version, license expression,
source provenance, and SHA-256 entry under `third_party/`. The bounded dashboard
build-emitter audit covers Vite, Rollup, esbuild, and compiler output from the
production `svelte` package. The separately pinned `release-toolchain.json`
covers the Rust sysroot/runtime and the musl, GCC, and LLVM inputs used for the
native archives. Re-audit those classifications and components when the
corresponding build path changes; Vite and Svelte configuration files are
notice fingerprints for that reason.

`just check` rebuilds the inventory offline and rejects stale notices, unused
overrides, workflow/toolchain version drift, missing components, changed
document hashes, missing license metadata, or missing text. Each server archive
includes the checked `THIRD_PARTY_NOTICES.txt` beside the binary.

## Prerelease checklist

1. Confirm the Apache-2.0 root and Python license copies are byte-identical and
   the public Python distribution and import name are both `epochdeck`.
2. Configure a `v*` repository tag ruleset that blocks tag updates and
   deletion, limits tag creation to release maintainers, and cannot be bypassed
   by ordinary pushes. Enable immutable releases in the repository settings.
   The workflow rejects an unprotected or GitHub-unverified tag; immutable
   release status is an external repository-administration check because the
   workflow token cannot read that setting.
3. Prepare `docs/releases/vX.Y.Z-alpha.N.md` and give the matching
   `CHANGELOG.md` heading its release date.
4. Update Cargo and dashboard SemVer plus the equivalent Python PEP 440 version.
5. Regenerate `Cargo.lock` and `python/uv.lock`.
6. Run `nix develop --command just bootstrap`.
7. Run `nix develop --command just release-ready-check`.
8. Run `nix develop --command just check`.
9. Confirm `git status --short` is empty and CI on `main` is green.
10. Merge the release pull request.
11. Run the `GitHub prerelease` workflow manually against the exact `main`
    commit. Confirm all four native build/test/archive jobs and the Python 3.11
    and 3.13 wheel-smoke matrix pass. Download the candidate, verify its exact
    six-payload manifest and checksums, extract and run the x86_64 Linux
    archive in a clean Debian LXC, and install the wheel in a clean Python 3.11
    environment. A manual run never creates a GitHub release.
12. Create and push a GitHub-verifiable signed annotated tag without moving any
    existing tag:

```bash
git tag -s v0.1.0-alpha.1 -m "EpochDeck 0.1.0-alpha.1"
git push origin v0.1.0-alpha.1
```

13. Wait for the tag-triggered GitHub prerelease workflow to finish.
14. Download every asset into one directory and verify it there:

    ```bash
    sha256sum --check SHA256SUMS
    ```

15. Deploy first to empty pre-alpha storage roots, run `epochdeck doctor`, log a
    sample run, inspect the dashboard, and practice backup/restore.
16. Preserve the previous binary and its exact storage roots for rollback.

GitHub releases in a private repository remain visible only to people who can
access that repository. Changing repository visibility is a separate,
deliberate release decision and is never performed by this workflow.

A failed, partially created release must not reuse or move its tag. Delete an
unconsumed failed draft and fix forward before retagging; if anyone may have
downloaded it, increment the prerelease number instead.
