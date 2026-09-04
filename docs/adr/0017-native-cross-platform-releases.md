# ADR 0017: Native cross-platform release builds

- Status: Accepted
- Date: 2026-09-04

## Context

EpochDeck's first prerelease built two Linux musl binaries on one x86_64 runner
through Zig and cargo-zigbuild. That produced portable Linux executables, but it
did not exercise the server on an ARM64 host and could not validate filesystem,
process, archive, or SDK behavior on macOS and Windows.

The server is intended to be an ordinary self-hosted executable. A release that
only proves that another target links is weaker than one that runs its storage
and HTTP smoke tests on the operating system and filesystem where users will
run it.

## Decision

GitHub prereleases build the embedded-dashboard server with ordinary Cargo on a
matching GitHub-hosted runner for each supported target:

- `x86_64-unknown-linux-musl` on x86_64 Linux;
- `aarch64-unknown-linux-musl` on ARM64 Linux;
- `x86_64-apple-darwin` on Intel macOS;
- `aarch64-apple-darwin` on Apple Silicon macOS; and
- `x86_64-pc-windows-msvc` on x86_64 Windows.

Linux continues to use musl so its archives do not depend on a host glibc
version. Native `musl-gcc` supplies the linker on each Linux architecture. The
release path does not install or invoke Zig, cargo-zigbuild, an emulator, or a
cross-compilation container. Windows statically links the MSVC C runtime so the
standalone archive does not require a separately installed Visual C++
Redistributable; its platform import audit rejects accidental dynamic CRT
dependencies.

Every target consumes the dashboard artifact produced by the repository-wide
verification job, builds from the locked Cargo graph, inspects the resulting
binary, runs it natively, and exercises bounded storage and HTTP behavior before
packaging it. Linux archives use `tar.gz`; macOS and Windows archives use ZIP.
The pure Python wheel is built once and that exact wheel is imported and
smoke-tested on every supported operating system.

Release assembly accepts an exact versioned filename set before generating
checksums. A protected, verified signed Git tag remains the publication
authority.

## Consequences

The release takes more runner time and has five independently failing build
jobs. In return, architecture, dynamic-link, filesystem, process, dashboard,
and packaging mistakes fail on their native platform before publication.

The macOS and Windows archives are checksummed prerelease artifacts, but native
builds do not themselves provide Apple notarization or Windows Authenticode.
Platform signing requires separately managed publisher identities and remains a
release-security task rather than a reason to cross-compile.

## Rejected alternatives

### Cross-compile all targets from Linux

Apple SDK licensing and the Windows MSVC toolchain make that path complex, and
emulation would still miss native filesystem and process behavior.

### Keep Zig only for Linux

Matching ARM64 Linux runners remove the original need for cross-linking. One
ordinary Cargo path is easier to audit, and native execution catches failures
that an x86_64 runner plus emulation can hide.

### Publish builds that only compile

Successful linking does not validate storage durability, archive permissions,
embedded assets, startup, persistence, or shutdown on the target platform.
