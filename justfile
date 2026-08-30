set shell := ["bash", "-euo", "pipefail", "-c"]

default:
  @just --list

bootstrap:
  cargo fetch --locked
  uv sync --project python --all-groups --locked
  pnpm --dir web install --frozen-lockfile

check: dependency-guard flake-check third-party-notices-check release-version-check workflow-check rust-check python-check web-check

dependency-guard:
  ./scripts/check-forbidden-dependencies.sh

flake-check:
  nix flake check --no-build --all-systems

third-party-notices-check:
  ./scripts/generate-third-party-notices.py --check

third-party-notices-generate:
  ./scripts/generate-third-party-notices.py

release-version-check:
  python3 scripts/check-release-version.py

release-ready-check:
  python3 scripts/check-release-version.py --require-prerelease --require-release-ready

workflow-check:
  actionlint

rust-check: dashboard-build
  cargo fmt --all --check
  cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
  cargo test --locked --workspace --all-targets --all-features

python-check:
  uv run --project python --locked ruff format --check
  uv run --project python --locked ruff check
  uv run --project python --locked mypy --config-file python/pyproject.toml python/src/epochdeck
  uv run --project python --locked pytest

web-check: dashboard-build
  pnpm --dir web check
  pnpm --dir web test

dashboard-build:
  pnpm --dir web build

single-binary: dashboard-build
  cargo build --release --locked -p epochdeck-server --features embedded-dashboard

format:
  cargo fmt --all
  uv run --project python ruff format
  uv run --project python ruff check --fix
  pnpm --dir web format

dev:
  python3 scripts/dev.py

benchmark-metrics rows="200000" metrics="180":
  cargo run --release -p epochdeck-storage --example metric_workload -- {{ rows }} {{ metrics }}
