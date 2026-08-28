set shell := ["bash", "-euo", "pipefail", "-c"]

default:
  @just --list

bootstrap:
  cargo fetch
  uv sync --project python --all-groups
  pnpm --dir web install

check: dependency-guard workflow-check rust-check python-check web-check

dependency-guard:
  ./scripts/check-forbidden-dependencies.sh

workflow-check:
  actionlint

rust-check:
  cargo fmt --all --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace

python-check:
  uv run --project python ruff format --check
  uv run --project python ruff check
  uv run --project python pytest

web-check:
  pnpm --dir web check
  pnpm --dir web test
  pnpm --dir web build

format:
  cargo fmt --all
  uv run --project python ruff format
  uv run --project python ruff check --fix
  pnpm --dir web format

dev-server:
  cargo run -p runloom-server

dev-web:
  pnpm --dir web dev
