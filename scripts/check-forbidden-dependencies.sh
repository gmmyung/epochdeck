#!/usr/bin/env bash
set -euo pipefail

pattern='(^|[^a-z])(gradio|huggingface[-_]?hub|huggingface|hf[-_]?xet|datasets)([^a-z]|$)'

if rg --line-number --ignore-case \
  --glob 'Cargo.toml' \
  --glob 'Cargo.lock' \
  --glob 'pyproject.toml' \
  --glob 'uv.lock' \
  --glob 'package.json' \
  --glob 'pnpm-lock.yaml' \
  "$pattern" .; then
  echo "Forbidden hosting-platform dependency detected." >&2
  exit 1
fi
