#!/usr/bin/env bash
set -euo pipefail

tag=${1:?usage: verify-release-tag.sh TAG EXPECTED_COMMIT}
expected_commit=${2:?usage: verify-release-tag.sh TAG EXPECTED_COMMIT}
: "${GH_TOKEN:?GH_TOKEN is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
  echo "release tag has an unsupported shape: $tag" >&2
  exit 1
fi

expected_commit=$(git rev-parse "${expected_commit}^{commit}")
tag_ref=$(gh api \
  --header "Accept: application/vnd.github+json" \
  --header "X-GitHub-Api-Version: 2022-11-28" \
  "repos/${GITHUB_REPOSITORY}/git/ref/tags/${tag}")
tag_type=$(jq -r '.object.type' <<<"$tag_ref")
tag_object=$(jq -r '.object.sha' <<<"$tag_ref")
if [[ "$tag_type" != "tag" || ! "$tag_object" =~ ^[0-9a-f]{40}$ ]]; then
  echo "release tag must be an annotated tag object" >&2
  exit 1
fi

tag_payload=$(gh api \
  --header "Accept: application/vnd.github+json" \
  --header "X-GitHub-Api-Version: 2022-11-28" \
  "repos/${GITHUB_REPOSITORY}/git/tags/${tag_object}")
if ! jq --exit-status \
  --arg tag "$tag" \
  --arg expected "$expected_commit" \
  '.tag == $tag and
   .object.type == "commit" and
   .object.sha == $expected and
   .verification.verified == true' \
  <<<"$tag_payload" >/dev/null; then
  actual_tag=$(jq -r '.tag // "missing"' <<<"$tag_payload")
  actual_commit=$(jq -r '.object.sha // "missing"' <<<"$tag_payload")
  verification=$(jq -r '.verification.reason // "missing"' <<<"$tag_payload")
  echo "release tag payload is invalid, unverified, or targets the wrong commit" >&2
  echo "expected tag:    $tag" >&2
  echo "signed tag:      $actual_tag" >&2
  echo "expected commit: $expected_commit" >&2
  echo "actual commit:   $actual_commit" >&2
  echo "verification:    $verification" >&2
  exit 1
fi

echo "$tag is a verified signed tag for $expected_commit"
