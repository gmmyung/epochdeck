#!/usr/bin/env python3
"""Generate deterministic notices for dependencies shipped in the server archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "THIRD_PARTY_NOTICES.txt"
OVERRIDES_PATH = ROOT / "third_party" / "license-overrides.json"
TOOLCHAIN_MANIFEST_PATH = ROOT / "third_party" / "release-toolchain.json"
TOOLCHAIN_DOCUMENT_ROOT = ROOT / "third_party" / "toolchain-runtime"
RELEASE_WORKFLOW_PATH = ROOT / ".github" / "workflows" / "prerelease.yml"
CARGO_MANIFEST = ROOT / "crates" / "epochdeck-server" / "Cargo.toml"
PNPM_LOCK = ROOT / "web" / "pnpm-lock.yaml"
INSTALLED_PNPM_LOCK = ROOT / "web" / "node_modules" / ".pnpm" / "lock.yaml"
RUST_TARGETS = (
    "aarch64-apple-darwin",
    "aarch64-unknown-linux-musl",
    "x86_64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-musl",
)
EXPECTED_WORKFLOW_RUNNERS = {
    "aarch64-apple-darwin": "macos-15",
    "aarch64-unknown-linux-musl": "ubuntu-24.04-arm",
    "x86_64-apple-darwin": "macos-15-intel",
    "x86_64-pc-windows-msvc": "windows-2022",
    "x86_64-unknown-linux-musl": "ubuntu-24.04",
}
DASHBOARD_CODE_EMITTERS = {
    "esbuild": (
        "covers esbuild's production minification/transformation output and any "
        "runtime helpers emitted into the production bundle"
    ),
    "rollup": (
        "conservatively covers Rollup-generated chunk wrappers and interoperability "
        "helpers in production bundles"
    ),
    "vite": (
        "covers Vite's modulepreload polyfill and any Vite runtime helpers emitted "
        "into the production bundle"
    ),
}
DASHBOARD_RUNTIME_COMPILERS = {
    "svelte": (
        "production dependency containing both the shipped Svelte runtime and the "
        "compiler that generates component payload"
    )
}
EXPECTED_TOOLCHAIN_PIN_VALUES = {
    "gcc-runtime": (
        "9.2.0",
        "a0c06cc27d2146b7d86758ffa236516c6143d62c",
    ),
    "llvm": (
        "19.1.7",
        "7e8c93c87c611f21d9bd95100563392f4c18bfe7",
    ),
    "musl": (
        "1.2.3",
        "7a43f6fea9081bdd53d8a11cef9e9fab0348c53d",
    ),
    "rust": (
        "1.85.0",
        "4d91de4e48198da2e33413efdcd9cd2cc0c46688",
    ),
}
EXPECTED_TOOLCHAIN_PINS = frozenset(EXPECTED_TOOLCHAIN_PIN_VALUES)
EXPECTED_TOOLCHAIN_COMPONENT_PINS = {
    "gcc-runtime": ("gcc-runtime", "rust"),
    "musl-libc": ("musl", "rust"),
    "rust-llvm-runtime": ("llvm", "rust"),
    "rust-standard-library": ("rust",),
}
EXPECTED_TOOLCHAIN_COMPONENT_DOCUMENTS = {
    "gcc-runtime": frozenset({"gcc-9.2.0/COPYING.RUNTIME", "gcc-9.2.0/COPYING3"}),
    "musl-libc": frozenset({"musl-1.2.3/COPYRIGHT"}),
    "rust-llvm-runtime": frozenset({"llvm/LICENSE.TXT", "rust-1.85.0/COPYRIGHT"}),
    "rust-standard-library": frozenset(
        {
            "rust-1.85.0/COPYRIGHT",
            "rust-1.85.0/LICENSE-APACHE",
            "rust-1.85.0/LICENSE-MIT",
        }
    ),
}
EXPECTED_TOOLCHAIN_COMPONENTS = frozenset(EXPECTED_TOOLCHAIN_COMPONENT_PINS)
NOTICE_INPUT_PATHS = (
    ROOT / "Cargo.toml",
    ROOT / "Cargo.lock",
    CARGO_MANIFEST,
    ROOT / "web" / "package.json",
    PNPM_LOCK,
    ROOT / "web" / "svelte.config.js",
    ROOT / "web" / "vite.config.ts",
    RELEASE_WORKFLOW_PATH,
    OVERRIDES_PATH,
    TOOLCHAIN_MANIFEST_PATH,
)

MAX_COMMAND_OUTPUT_BYTES = 16 * 1024 * 1024
MAX_DEPENDENCIES = 2_048
MAX_DOCUMENTS_PER_DEPENDENCY = 32
MAX_LICENSE_DOCUMENT_BYTES = 512 * 1024
MAX_UNIQUE_DOCUMENTS = 4_096
MAX_NOTICE_BYTES = 8 * 1024 * 1024
COMMAND_TIMEOUT_SECONDS = 180

LICENSE_FILE_RE = re.compile(
    r"^(?:licen[cs]e|copying|notice)(?:[._-].*)?$|^unlicense$",
    re.IGNORECASE,
)
SAFE_FIELD_RE = re.compile(r"^[^\x00-\x1f\x7f]+$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class NoticeError(RuntimeError):
    """A dependency could not be represented safely in the notice file."""


@dataclass(frozen=True)
class LicenseDocument:
    label: str
    source: str
    provenance: str
    text: str

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Dependency:
    ecosystem: str
    category: str
    name: str
    version: str
    license_expression: str
    source: str
    coverage: str
    documents: tuple[LicenseDocument, ...]

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.ecosystem, self.name, self.version)

    @property
    def display_name(self) -> str:
        return f"{self.ecosystem}: {self.name} {self.version}"


@dataclass(frozen=True)
class Override:
    ecosystem: str
    name: str
    version: str
    license_expression: str
    documents: tuple[dict[str, str], ...]

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.ecosystem, self.name, self.version)


def _field(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value or not SAFE_FIELD_RE.fullmatch(value):
        raise NoticeError(
            f"invalid {description}: expected a non-empty single-line string"
        )
    return value


def _run_json(command: list[str], description: str) -> Any:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as error:
        raise NoticeError(f"{description} requires {command[0]!r} on PATH") from error
    except subprocess.TimeoutExpired as error:
        raise NoticeError(
            f"{description} exceeded the {COMMAND_TIMEOUT_SECONDS}-second bound"
        ) from error

    if (
        len(result.stdout) > MAX_COMMAND_OUTPUT_BYTES
        or len(result.stderr) > MAX_COMMAND_OUTPUT_BYTES
    ):
        raise NoticeError(f"{description} exceeded the command-output bound")
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise NoticeError(
            f"{description} failed: {detail or f'exit {result.returncode}'}"
        )
    try:
        return json.loads(result.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise NoticeError(f"{description} returned invalid JSON") from error


def _normalize_text(raw: bytes, description: str) -> str:
    if not raw or len(raw) > MAX_LICENSE_DOCUMENT_BYTES:
        raise NoticeError(
            f"{description} must be between 1 and {MAX_LICENSE_DOCUMENT_BYTES} bytes"
        )
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise NoticeError(f"{description} is not UTF-8") from error
    text = text.replace("\r\n", "\n").replace("\r", "\n").rstrip() + "\n"
    if not text.strip():
        raise NoticeError(f"{description} contains no license text")
    return text


def _document_paths(root: Path, explicit: str | None) -> list[Path]:
    resolved_root = root.resolve(strict=True)
    paths = {
        path.resolve(strict=True)
        for path in resolved_root.iterdir()
        if path.is_file() and LICENSE_FILE_RE.fullmatch(path.name)
    }
    if explicit:
        explicit_path = Path(explicit)
        if not explicit_path.is_absolute():
            explicit_path = resolved_root / explicit_path
        explicit_path = explicit_path.resolve(strict=True)
        paths.add(explicit_path)
    if len(paths) > MAX_DOCUMENTS_PER_DEPENDENCY:
        raise NoticeError(f"{root} has too many candidate license documents")
    for path in paths:
        try:
            path.relative_to(resolved_root)
        except ValueError as error:
            raise NoticeError(
                f"license document escapes package root: {path}"
            ) from error
    return sorted(paths, key=lambda path: path.name.encode("utf-8"))


def _packaged_documents(
    root: Path,
    *,
    explicit: str | None,
    ecosystem: str,
    name: str,
    version: str,
) -> tuple[LicenseDocument, ...]:
    documents = []
    for path in _document_paths(root, explicit):
        documents.append(
            LicenseDocument(
                label=path.name,
                source=f"packaged with {ecosystem} dependency {name} {version}",
                provenance="verbatim dependency package file",
                text=_normalize_text(
                    path.read_bytes(), f"{name} {version}/{path.name}"
                ),
            )
        )
    return tuple(documents)


def _load_overrides() -> dict[tuple[str, str, str], Override]:
    try:
        raw = json.loads(OVERRIDES_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise NoticeError(
            f"missing override manifest: {OVERRIDES_PATH.relative_to(ROOT)}"
        ) from error
    except json.JSONDecodeError as error:
        raise NoticeError("license override manifest is invalid JSON") from error
    if not isinstance(raw, dict) or raw.get("schema") != 1:
        raise NoticeError("license override manifest must use schema 1")
    entries = raw.get("overrides")
    if not isinstance(entries, list) or len(entries) > MAX_DEPENDENCIES:
        raise NoticeError("license override manifest has an invalid overrides list")

    overrides: dict[tuple[str, str, str], Override] = {}
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise NoticeError(f"license override {index} is not an object")
        documents = entry.get("documents")
        if (
            not isinstance(documents, list)
            or not documents
            or len(documents) > MAX_DOCUMENTS_PER_DEPENDENCY
            or not all(isinstance(document, dict) for document in documents)
        ):
            raise NoticeError(f"license override {index} has invalid documents")
        override = Override(
            ecosystem=_field(entry.get("ecosystem"), f"override {index} ecosystem"),
            name=_field(entry.get("name"), f"override {index} name"),
            version=_field(entry.get("version"), f"override {index} version"),
            license_expression=_field(
                entry.get("license_expression"), f"override {index} license expression"
            ),
            documents=tuple(documents),
        )
        if override.identity in overrides:
            raise NoticeError(f"duplicate license override for {override.identity}")
        overrides[override.identity] = override
    return overrides


def _audited_documents(
    entries: tuple[dict[str, str], ...],
    *,
    document_root: Path,
    description: str,
    used_paths: set[Path] | None = None,
) -> tuple[LicenseDocument, ...]:
    documents = []
    resolved_root = document_root.resolve(strict=True)
    seen_paths: set[Path] = set()
    for entry in entries:
        if set(entry) != {"path", "sha256", "source", "provenance"}:
            raise NoticeError(f"{description} document has unexpected fields")
        relative_path = Path(_field(entry.get("path"), f"{description} document path"))
        if relative_path.is_absolute():
            raise NoticeError(f"{description} path must be relative: {relative_path}")
        path = (resolved_root / relative_path).resolve(strict=True)
        try:
            path.relative_to(resolved_root)
        except ValueError as error:
            raise NoticeError(
                f"{description} path escapes document root: {relative_path}"
            ) from error
        if path in seen_paths:
            raise NoticeError(f"{description} repeats document {relative_path}")
        seen_paths.add(path)
        if used_paths is not None:
            used_paths.add(path)
        expected_digest = _field(entry.get("sha256"), f"{description} document SHA-256")
        if not SHA256_RE.fullmatch(expected_digest):
            raise NoticeError(f"invalid {description} SHA-256 for {relative_path}")
        raw = path.read_bytes()
        actual_digest = hashlib.sha256(raw).hexdigest()
        if actual_digest != expected_digest:
            raise NoticeError(
                f"{description} hash mismatch for {relative_path}: "
                f"expected {expected_digest}, found {actual_digest}"
            )
        documents.append(
            LicenseDocument(
                label=relative_path.as_posix(),
                source=_field(entry.get("source"), f"{description} document source"),
                provenance=_field(
                    entry.get("provenance"), f"{description} document provenance"
                ),
                text=_normalize_text(raw, f"{description} document {relative_path}"),
            )
        )
    return tuple(documents)


def _override_documents(override: Override) -> tuple[LicenseDocument, ...]:
    return _audited_documents(
        override.documents,
        document_root=ROOT / "third_party" / "license-overrides",
        description="override",
    )


def _resolve_documents(
    dependency: Dependency,
    overrides: dict[tuple[str, str, str], Override],
    used_overrides: set[tuple[str, str, str]],
) -> Dependency:
    if dependency.documents:
        return dependency
    override = overrides.get(dependency.identity)
    if override is None:
        raise NoticeError(
            f"{dependency.display_name} has license metadata "
            f"({dependency.license_expression}) but no packaged license text or audited override"
        )
    if override.license_expression != dependency.license_expression:
        raise NoticeError(
            f"license metadata changed for {dependency.display_name}: "
            f"override has {override.license_expression!r}, dependency has "
            f"{dependency.license_expression!r}"
        )
    used_overrides.add(override.identity)
    return Dependency(
        ecosystem=dependency.ecosystem,
        category=dependency.category,
        name=dependency.name,
        version=dependency.version,
        license_expression=dependency.license_expression,
        source=dependency.source,
        coverage=dependency.coverage,
        documents=_override_documents(override),
    )


def _cargo_dependencies() -> list[Dependency]:
    cargo = os.environ.get("CARGO", "cargo")
    package_by_id: dict[str, dict[str, Any]] = {}
    selected_ids: set[str] = set()
    workspace_ids: set[str] = set()

    for target in RUST_TARGETS:
        metadata = _run_json(
            [
                cargo,
                "metadata",
                "--manifest-path",
                str(CARGO_MANIFEST),
                "--format-version",
                "1",
                "--filter-platform",
                target,
                "--features",
                "embedded-dashboard",
                "--locked",
                "--offline",
            ],
            f"Cargo metadata for {target}",
        )
        if not isinstance(metadata, dict):
            raise NoticeError(f"Cargo metadata for {target} is not an object")
        packages = metadata.get("packages")
        resolve = metadata.get("resolve")
        if not isinstance(packages, list) or not isinstance(resolve, dict):
            raise NoticeError(f"Cargo metadata for {target} is incomplete")
        if len(packages) > MAX_DEPENDENCIES:
            raise NoticeError(f"Cargo dependency graph for {target} exceeds its bound")
        for package in packages:
            if not isinstance(package, dict) or not isinstance(package.get("id"), str):
                raise NoticeError(f"Cargo metadata for {target} has an invalid package")
            package_by_id[package["id"]] = package
        members = metadata.get("workspace_members")
        if not isinstance(members, list) or not all(
            isinstance(item, str) for item in members
        ):
            raise NoticeError(
                f"Cargo metadata for {target} has invalid workspace members"
            )
        workspace_ids.update(members)

        root = resolve.get("root")
        nodes = resolve.get("nodes")
        if not isinstance(root, str) or not isinstance(nodes, list):
            raise NoticeError(
                f"Cargo metadata for {target} has no package resolution root"
            )
        node_by_id = {
            node["id"]: node
            for node in nodes
            if isinstance(node, dict) and isinstance(node.get("id"), str)
        }
        if len(node_by_id) != len(nodes) or root not in node_by_id:
            raise NoticeError(
                f"Cargo metadata for {target} has an invalid resolution graph"
            )

        pending = [root]
        target_ids: set[str] = set()
        while pending:
            package_id = pending.pop()
            if package_id in target_ids:
                continue
            if len(target_ids) >= MAX_DEPENDENCIES:
                raise NoticeError(
                    f"Cargo dependency graph for {target} exceeds its bound"
                )
            target_ids.add(package_id)
            node = node_by_id.get(package_id)
            if node is None:
                raise NoticeError(f"Cargo dependency node is missing: {package_id}")
            dependencies = node.get("deps")
            if not isinstance(dependencies, list):
                raise NoticeError(
                    f"Cargo dependency node has invalid edges: {package_id}"
                )
            for edge in dependencies:
                if not isinstance(edge, dict) or not isinstance(edge.get("pkg"), str):
                    raise NoticeError(
                        f"Cargo dependency node has an invalid edge: {package_id}"
                    )
                kinds = edge.get("dep_kinds")
                if not isinstance(kinds, list):
                    raise NoticeError(
                        f"Cargo dependency edge has invalid kinds: {package_id}"
                    )
                if any(
                    not isinstance(kind, dict)
                    or kind.get("kind") not in {None, "normal", "dev", "build"}
                    for kind in kinds
                ):
                    raise NoticeError(
                        f"Cargo dependency edge has an unknown kind: {package_id}"
                    )
                if not kinds or any(kind.get("kind") != "dev" for kind in kinds):
                    pending.append(edge["pkg"])
        selected_ids.update(target_ids)

    dependencies = []
    for package_id in sorted(selected_ids):
        package = package_by_id.get(package_id)
        if package is None:
            raise NoticeError(f"Cargo package metadata is missing: {package_id}")
        if package_id in workspace_ids:
            continue
        source = package.get("source")
        if source is None:
            raise NoticeError(
                f"unclassified external path dependency in server graph: {package.get('name')}"
            )
        license_expression = _field(package.get("license"), "Cargo license expression")
        name = _field(package.get("name"), "Cargo package name")
        version = _field(package.get("version"), "Cargo package version")
        manifest = Path(_field(package.get("manifest_path"), "Cargo manifest path"))
        repository = package.get("repository")
        dependency_source = (
            repository if isinstance(repository, str) and repository else source
        )
        dependencies.append(
            Dependency(
                ecosystem="cargo",
                category="cargo",
                name=name,
                version=version,
                license_expression=license_expression,
                source=_field(dependency_source, f"source for Cargo package {name}"),
                coverage=(
                    "selected by the release server's non-development target graph; "
                    "build dependencies are retained conservatively"
                ),
                documents=_packaged_documents(
                    manifest.parent,
                    explicit=package.get("license_file"),
                    ecosystem="Cargo",
                    name=name,
                    version=version,
                ),
            )
        )
    return dependencies


def _repository(package: dict[str, Any], fallback: str) -> str:
    repository = package.get("repository")
    if isinstance(repository, str) and repository:
        return repository
    if isinstance(repository, dict) and isinstance(repository.get("url"), str):
        return _field(repository["url"], "pnpm repository URL")
    homepage = package.get("homepage")
    if isinstance(homepage, str) and homepage:
        return homepage
    return fallback


def _pnpm_report_dependencies(
    report: Any,
    *,
    category: str,
    description: str,
    allowed_names: frozenset[str] | None,
) -> tuple[dict[tuple[str, str, str], Dependency], set[str]]:
    if not isinstance(report, dict):
        raise NoticeError(f"{description} is not an object")
    dependencies_by_id: dict[tuple[str, str, str], Dependency] = {}
    selected_names: set[str] = set()
    path_count = 0
    for group_license, entries in report.items():
        license_group = _field(group_license, "pnpm license group")
        if license_group.upper() in {"UNKNOWN", "UNLICENSED"}:
            raise NoticeError(
                f"pnpm reported a dependency with {license_group} metadata"
            )
        if not isinstance(entries, list):
            raise NoticeError(f"pnpm license group {license_group} is not a list")
        for entry in entries:
            if not isinstance(entry, dict):
                raise NoticeError(
                    f"pnpm license group {license_group} has an invalid entry"
                )
            entry_name = _field(entry.get("name"), "pnpm package name")
            if allowed_names is not None and entry_name not in allowed_names:
                continue
            selected_names.add(entry_name)
            entry_license = _field(
                entry.get("license"), f"license for pnpm package {entry_name}"
            )
            if entry_license != license_group:
                raise NoticeError(f"pnpm license grouping disagrees for {entry_name}")
            paths = entry.get("paths")
            if not isinstance(paths, list) or not paths:
                raise NoticeError(f"pnpm package {entry_name} has no installed path")
            reported_versions = entry.get("versions")
            if not isinstance(reported_versions, list) or not all(
                isinstance(version, str) and version for version in reported_versions
            ):
                raise NoticeError(f"pnpm package {entry_name} has invalid versions")
            path_count += len(paths)
            if path_count > MAX_DEPENDENCIES:
                raise NoticeError(f"{description} exceeds its dependency bound")

            installed_versions: set[str] = set()
            for raw_path in paths:
                root = Path(
                    _field(raw_path, f"installed path for {entry_name}")
                ).resolve(strict=True)
                try:
                    root.relative_to(
                        (ROOT / "web" / "node_modules").resolve(strict=True)
                    )
                except ValueError as error:
                    raise NoticeError(
                        f"pnpm package path escapes web/node_modules: {entry_name}"
                    ) from error
                try:
                    package = json.loads(
                        (root / "package.json").read_text(encoding="utf-8")
                    )
                except (FileNotFoundError, json.JSONDecodeError) as error:
                    raise NoticeError(
                        f"invalid installed package metadata for {entry_name}"
                    ) from error
                if not isinstance(package, dict):
                    raise NoticeError(
                        f"invalid installed package metadata for {entry_name}"
                    )
                name = _field(package.get("name"), "installed pnpm package name")
                version = _field(
                    package.get("version"), f"version for pnpm package {name}"
                )
                installed_versions.add(version)
                license_expression = _field(
                    package.get("license"),
                    f"license expression for pnpm package {name}",
                )
                if name != entry_name or license_expression != entry_license:
                    raise NoticeError(
                        f"pnpm inventory disagrees with package.json for {entry_name}"
                    )
                dependency = Dependency(
                    ecosystem="pnpm",
                    category=category,
                    name=name,
                    version=version,
                    license_expression=license_expression,
                    source=_repository(package, f"npm:{name}@{version}"),
                    coverage=(
                        DASHBOARD_CODE_EMITTERS[name]
                        if category == "dashboard-build-emitter"
                        else DASHBOARD_RUNTIME_COMPILERS.get(
                            name,
                            "production dependency included in the embedded dashboard bundle",
                        )
                    ),
                    documents=_packaged_documents(
                        root,
                        explicit=None,
                        ecosystem="pnpm",
                        name=name,
                        version=version,
                    ),
                )
                previous = dependencies_by_id.get(dependency.identity)
                if previous is not None and previous != dependency:
                    raise NoticeError(
                        f"conflicting installed copies of pnpm package {name} {version}"
                    )
                dependencies_by_id[dependency.identity] = dependency
            if installed_versions != set(reported_versions):
                raise NoticeError(
                    f"pnpm version inventory disagrees with installed paths for {entry_name}"
                )

    return dependencies_by_id, selected_names


def _pnpm_dependencies() -> list[Dependency]:
    try:
        expected_lock = PNPM_LOCK.read_bytes()
        installed_lock = INSTALLED_PNPM_LOCK.read_bytes()
    except FileNotFoundError as error:
        raise NoticeError(
            "dashboard dependencies are not installed; run `just bootstrap` first"
        ) from error
    if expected_lock != installed_lock:
        raise NoticeError(
            "installed dashboard dependencies do not match web/pnpm-lock.yaml; "
            "run `pnpm --dir web install --frozen-lockfile`"
        )

    pnpm = os.environ.get("PNPM", "pnpm")
    runtime, runtime_names = _pnpm_report_dependencies(
        _run_json(
            [pnpm, "--dir", "web", "licenses", "list", "--prod", "--json"],
            "pnpm production license inventory",
        ),
        category="dashboard-runtime",
        description="pnpm production license inventory",
        allowed_names=None,
    )
    missing_runtime_compilers = set(DASHBOARD_RUNTIME_COMPILERS) - runtime_names
    if missing_runtime_compilers:
        raise NoticeError(
            "dashboard runtime/compiler inventory drifted; missing: "
            + ", ".join(sorted(missing_runtime_compilers))
        )
    emitters, selected_emitters = _pnpm_report_dependencies(
        _run_json(
            [pnpm, "--dir", "web", "licenses", "list", "--dev", "--json"],
            "pnpm development license inventory",
        ),
        category="dashboard-build-emitter",
        description="pnpm development license inventory",
        allowed_names=frozenset(DASHBOARD_CODE_EMITTERS),
    )
    expected_emitters = set(DASHBOARD_CODE_EMITTERS)
    if selected_emitters != expected_emitters:
        missing = ", ".join(sorted(expected_emitters - selected_emitters)) or "none"
        unexpected = ", ".join(sorted(selected_emitters - expected_emitters)) or "none"
        raise NoticeError(
            f"dashboard code-emitter inventory drifted; missing: {missing}; "
            f"unexpected: {unexpected}"
        )
    overlap = set(runtime) & set(emitters)
    if overlap:
        raise NoticeError(
            f"dashboard runtime/code-emitter inventory overlaps: {overlap}"
        )

    return [*runtime.values(), *emitters.values()]


def _workflow_step(workflow: str, name: str) -> str:
    marker = f"      - name: {name}\n"
    if workflow.count(marker) != 1:
        raise NoticeError(f"release workflow must contain exactly one {name!r} step")
    remainder = workflow.split(marker, 1)[1]
    return remainder.split("\n      - name: ", 1)[0]


def _single_workflow_value(pattern: str, text: str, description: str) -> str:
    values = re.findall(pattern, text, flags=re.MULTILINE)
    if len(values) != 1:
        raise NoticeError(f"release workflow must declare exactly one {description}")
    return values[0]


def _toolchain_dependencies() -> list[Dependency]:
    try:
        manifest = json.loads(TOOLCHAIN_MANIFEST_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise NoticeError(
            f"missing toolchain manifest: {TOOLCHAIN_MANIFEST_PATH.relative_to(ROOT)}"
        ) from error
    except json.JSONDecodeError as error:
        raise NoticeError("release toolchain manifest is invalid JSON") from error
    if not isinstance(manifest, dict) or manifest.get("schema") != 1:
        raise NoticeError("release toolchain manifest must use schema 1")
    if set(manifest) != {"schema", "targets", "pins", "components"}:
        raise NoticeError("release toolchain manifest has unexpected fields")

    pins = manifest.get("pins")
    if not isinstance(pins, dict) or set(pins) != EXPECTED_TOOLCHAIN_PINS:
        raise NoticeError(
            "release toolchain pins must be exactly: "
            + ", ".join(sorted(EXPECTED_TOOLCHAIN_PINS))
        )
    normalized_pins: dict[str, dict[str, str]] = {}
    for name, pin in pins.items():
        if not isinstance(pin, dict):
            raise NoticeError(f"release toolchain pin {name!r} is not an object")
        if set(pin) != {"version", "revision", "source"}:
            raise NoticeError(f"release toolchain pin {name!r} has unexpected fields")
        normalized_pins[name] = {
            "version": _field(pin.get("version"), f"{name} version"),
            "revision": _field(pin.get("revision"), f"{name} revision"),
            "source": _field(pin.get("source"), f"{name} source"),
        }
        actual_pin = (
            normalized_pins[name]["version"],
            normalized_pins[name]["revision"],
        )
        expected_pin = EXPECTED_TOOLCHAIN_PIN_VALUES[name]
        if actual_pin != expected_pin:
            raise NoticeError(
                f"release toolchain pin {name!r} drifted: "
                f"found {actual_pin!r}, expected {expected_pin!r}"
            )

    targets = manifest.get("targets")
    if targets != list(RUST_TARGETS):
        raise NoticeError(
            "release toolchain targets must exactly match the notice generator's targets"
        )

    workflow = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    rust_version = normalized_pins["rust"]["version"]
    actual_rust = _single_workflow_value(
        r"^\s*toolchain:\s*([^\s#]+)",
        _workflow_step(workflow, "Install Rust 1.85"),
        "Rust toolchain version",
    )
    linked_rust = _single_workflow_value(
        r"cargo \+([^\s]+) build",
        _workflow_step(workflow, "Build release server"),
        "Rust version in the cargo build command",
    )
    actual = (actual_rust, linked_rust)
    expected = (rust_version, rust_version)
    if actual != expected:
        raise NoticeError(
            "release workflow toolchain pins disagree with third_party/release-toolchain.json: "
            f"found {actual!r}, expected {expected!r}"
        )
    if re.search(r"(?:cargo-zigbuild|setup-zig|\bzigbuild\b)", workflow, re.IGNORECASE):
        raise NoticeError(
            "release workflow must use ordinary Cargo without Zig tooling"
        )
    workflow_matrix = dict(
        re.findall(
            r"^\s*- runner:\s*([^\s#]+)\s*\n\s+target:\s*([^\s#]+)\s*$",
            workflow,
            re.MULTILINE,
        )
    )
    workflow_targets = set(workflow_matrix.values())
    if workflow_targets != set(RUST_TARGETS):
        raise NoticeError(
            f"release workflow targets drifted: found {sorted(workflow_targets)!r}"
        )
    actual_runners = {target: runner for runner, target in workflow_matrix.items()}
    if actual_runners != EXPECTED_WORKFLOW_RUNNERS:
        raise NoticeError(
            f"release workflow native runners drifted: found {actual_runners!r}"
        )

    components = manifest.get("components")
    if not isinstance(components, list) or len(components) > MAX_DEPENDENCIES:
        raise NoticeError("release toolchain manifest has invalid components")
    component_ids = {
        component.get("id")
        for component in components
        if isinstance(component, dict) and isinstance(component.get("id"), str)
    }
    if component_ids != EXPECTED_TOOLCHAIN_COMPONENTS or len(component_ids) != len(
        components
    ):
        raise NoticeError(
            "release toolchain components must be exactly: "
            + ", ".join(sorted(EXPECTED_TOOLCHAIN_COMPONENTS))
        )

    dependencies = []
    covered_pins: set[str] = set()
    used_document_paths: set[Path] = set()
    for component in components:
        if not isinstance(component, dict):
            raise NoticeError("release toolchain component is not an object")
        if set(component) != {
            "id",
            "pins",
            "name",
            "version",
            "license_expression",
            "source",
            "coverage",
            "documents",
        }:
            raise NoticeError("release toolchain component has unexpected fields")
        component_id = _field(component.get("id"), "toolchain component id")
        component_pins = component.get("pins")
        if (
            not isinstance(component_pins, list)
            or not component_pins
            or not all(
                isinstance(pin, str) and pin in normalized_pins
                for pin in component_pins
            )
            or len(set(component_pins)) != len(component_pins)
        ):
            raise NoticeError(f"toolchain component {component_id} has invalid pins")
        if tuple(component_pins) != EXPECTED_TOOLCHAIN_COMPONENT_PINS[component_id]:
            raise NoticeError(f"toolchain component {component_id} pin set drifted")
        version = _field(component.get("version"), f"{component_id} version")
        for pin in component_pins:
            if normalized_pins[pin]["version"] not in version:
                raise NoticeError(
                    f"toolchain component {component_id} version does not include its {pin} pin"
                )
        covered_pins.update(component_pins)
        documents = component.get("documents")
        if (
            not isinstance(documents, list)
            or not documents
            or len(documents) > MAX_DOCUMENTS_PER_DEPENDENCY
            or not all(isinstance(document, dict) for document in documents)
        ):
            raise NoticeError(
                f"toolchain component {component_id} has invalid documents"
            )
        document_paths = [document.get("path") for document in documents]
        if (
            not all(isinstance(path, str) for path in document_paths)
            or frozenset(document_paths)
            != EXPECTED_TOOLCHAIN_COMPONENT_DOCUMENTS[component_id]
        ):
            raise NoticeError(
                f"toolchain component {component_id} document set drifted"
            )
        dependencies.append(
            Dependency(
                ecosystem="toolchain",
                category="toolchain",
                name=_field(component.get("name"), f"{component_id} name"),
                version=version,
                license_expression=_field(
                    component.get("license_expression"),
                    f"{component_id} license expression",
                ),
                source=_field(component.get("source"), f"{component_id} source"),
                coverage=_field(
                    component.get("coverage"), f"{component_id} coverage rationale"
                ),
                documents=_audited_documents(
                    tuple(documents),
                    document_root=TOOLCHAIN_DOCUMENT_ROOT,
                    description=f"toolchain component {component_id}",
                    used_paths=used_document_paths,
                ),
            )
        )
    if covered_pins != EXPECTED_TOOLCHAIN_PINS:
        raise NoticeError(
            f"release toolchain components do not cover pins: "
            f"{sorted(EXPECTED_TOOLCHAIN_PINS - covered_pins)!r}"
        )
    document_entries = list(TOOLCHAIN_DOCUMENT_ROOT.rglob("*"))
    if any(entry.is_symlink() for entry in document_entries):
        raise NoticeError("release toolchain document tree must not contain symlinks")
    available_document_paths = {
        entry.resolve(strict=True) for entry in document_entries if entry.is_file()
    }
    if len(available_document_paths) > MAX_UNIQUE_DOCUMENTS:
        raise NoticeError("release toolchain document inventory exceeds its bound")
    if used_document_paths != available_document_paths:
        unused = sorted(
            path.relative_to(TOOLCHAIN_DOCUMENT_ROOT.resolve()).as_posix()
            for path in available_document_paths - used_document_paths
        )
        missing = sorted(
            path.relative_to(TOOLCHAIN_DOCUMENT_ROOT.resolve()).as_posix()
            for path in used_document_paths - available_document_paths
        )
        raise NoticeError(
            f"release toolchain document inventory drifted; "
            f"unused: {unused!r}; missing: {missing!r}"
        )
    return dependencies


def _render(dependencies: Iterable[Dependency]) -> str:
    ordered = sorted(dependencies, key=lambda item: item.identity)
    if not ordered or len(ordered) > MAX_DEPENDENCIES:
        raise NoticeError("combined dependency inventory is empty or exceeds its bound")
    if len({dependency.identity for dependency in ordered}) != len(ordered):
        raise NoticeError("combined dependency inventory contains duplicate identities")

    document_text: dict[str, str] = {}
    document_users: dict[str, set[str]] = {}
    document_origins: dict[str, set[str]] = {}
    for dependency in ordered:
        if not dependency.documents:
            raise NoticeError(f"{dependency.display_name} has no license document")
        for document in dependency.documents:
            digest = document.digest
            existing = document_text.setdefault(digest, document.text)
            if existing != document.text:
                raise NoticeError(
                    f"SHA-256 collision while processing {dependency.display_name}"
                )
            document_users.setdefault(digest, set()).add(dependency.display_name)
            document_origins.setdefault(digest, set()).add(
                f"{document.label} | {document.source} | {document.provenance}"
            )
    if len(document_text) > MAX_UNIQUE_DOCUMENTS:
        raise NoticeError("unique license-document inventory exceeds its bound")

    lines = [
        "EPOCHDECK THIRD-PARTY NOTICES",
        "===========================",
        "",
        "This file is generated by scripts/generate-third-party-notices.py.",
        "Do not edit it by hand. Regenerate it from the locked dependency inputs.",
        "",
        "Scope",
        "-----",
        "",
        "- The non-development Cargo dependency closure of epochdeck-server with the",
        "  embedded-dashboard feature for the five native release targets:",
        "  Linux x86_64/aarch64, macOS x86_64/aarch64, and Windows x86_64.",
        "  Build dependencies are included conservatively.",
        "- Production dependencies reported by pnpm for the embedded dashboard.",
        "- Vite, Rollup, and esbuild as an explicit bounded set of production",
        "  code-emitting dashboard build tools. Vite emits the modulepreload",
        "  polyfill, Rollup generates chunk/interoperability helpers, and esbuild",
        "  performs production transformation and minification.",
        "- Pinned Rust standard-library, panic, compiler-builtins, musl, GCC runtime,",
        "  and LLVM runtime coverage for the native Cargo release toolchains.",
        "  The Svelte compiler is covered by the inventoried svelte package itself;",
        "  vite-plugin-svelte only orchestrates the production transform and contributes",
        "  no separately shipped runtime bytes.",
        "- EpochDeck's own license is intentionally outside this third-party notice.",
        "",
        f"Inventory: {sum(item.ecosystem == 'cargo' for item in ordered)} Cargo packages,",
        f"           {sum(item.category == 'dashboard-runtime' for item in ordered)} dashboard runtime packages,",
        f"           {sum(item.category == 'dashboard-build-emitter' for item in ordered)} dashboard code emitters,",
        f"           {sum(item.ecosystem == 'toolchain' for item in ordered)} toolchain/runtime components,",
        f"           {len(document_text)} unique license documents.",
        "",
        "Locked input fingerprints:",
        *(
            f"- {path.relative_to(ROOT).as_posix()}: "
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}"
            for path in NOTICE_INPUT_PATHS
        ),
        "",
        "DEPENDENCY INDEX",
        "================",
        "",
    ]

    sections = (
        ("cargo", "Cargo dependencies"),
        ("dashboard-runtime", "Dashboard runtime dependencies (pnpm)"),
        ("dashboard-build-emitter", "Dashboard code-emitting build tools"),
        ("toolchain", "Pinned release toolchain and runtime"),
    )
    for category, heading in sections:
        scoped = [item for item in ordered if item.category == category]
        lines.extend((f"{heading} ({len(scoped)})", "-" * (len(heading) + 4), ""))
        for dependency in scoped:
            lines.extend(
                (
                    f"- {dependency.name} {dependency.version}",
                    f"  License expression: {dependency.license_expression}",
                    f"  Source: {dependency.source}",
                    f"  Coverage rationale: {dependency.coverage}",
                    "  License documents:",
                )
            )
            for document in sorted(
                dependency.documents, key=lambda item: (item.label, item.digest)
            ):
                lines.append(f"    - {document.label} (SHA-256: {document.digest})")
            lines.append("")

    lines.extend(("LICENSE DOCUMENTS", "=================", ""))
    for digest in sorted(document_text):
        lines.extend(
            (
                "=" * 78,
                f"SHA-256: {digest}",
                "Used by:",
                *(f"- {user}" for user in sorted(document_users[digest])),
                "Document provenance:",
                *(f"- {origin}" for origin in sorted(document_origins[digest])),
                "=" * 78,
                "",
                document_text[digest].rstrip(),
                "",
            )
        )
    rendered = "\n".join(lines).rstrip() + "\n"
    if len(rendered.encode("utf-8")) > MAX_NOTICE_BYTES:
        raise NoticeError("generated notice exceeds its output-size bound")
    return rendered


def generate() -> str:
    overrides = _load_overrides()
    used_overrides: set[tuple[str, str, str]] = set()
    dependencies = [
        *_cargo_dependencies(),
        *_pnpm_dependencies(),
        *_toolchain_dependencies(),
    ]
    dependencies = [
        _resolve_documents(dependency, overrides, used_overrides)
        for dependency in dependencies
    ]
    unused = sorted(set(overrides) - used_overrides)
    if unused:
        rendered = ", ".join("/".join(identity) for identity in unused)
        raise NoticeError(f"unused license overrides must be removed: {rendered}")
    return _render(dependencies)


def _first_difference(expected: str, actual: str) -> str:
    expected_lines = expected.splitlines()
    actual_lines = actual.splitlines()
    for number in range(max(len(expected_lines), len(actual_lines))):
        expected_line = (
            expected_lines[number] if number < len(expected_lines) else "<end of file>"
        )
        actual_line = (
            actual_lines[number] if number < len(actual_lines) else "<end of file>"
        )
        if expected_line != actual_line:
            return (
                f"first difference at line {number + 1}:\n"
                f"  generated: {expected_line[:240]}\n"
                f"  checked in: {actual_line[:240]}"
            )
    return "files differ"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the checked-in notice instead of replacing it",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help=f"output path (default: {OUTPUT_PATH.relative_to(ROOT)})",
    )
    args = parser.parse_args()

    try:
        rendered = generate()
        if args.check:
            try:
                if args.output.stat().st_size > MAX_NOTICE_BYTES:
                    raise NoticeError(f"{args.output} exceeds the notice-size bound")
                current = args.output.read_text(encoding="utf-8")
            except FileNotFoundError as error:
                raise NoticeError(
                    f"missing {args.output}; run `just third-party-notices-generate`"
                ) from error
            if current != rendered:
                raise NoticeError(
                    f"{args.output} is stale; run `just third-party-notices-generate`\n"
                    f"{_first_difference(rendered, current)}"
                )
            print(f"Verified {args.output} ({len(rendered.encode('utf-8'))} bytes).")
            return 0

        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=args.output.parent,
                prefix=f".{args.output.name}.",
                delete=False,
            ) as temporary:
                temporary.write(rendered)
                temporary_path = Path(temporary.name)
            temporary_path.chmod(0o644)
            temporary_path.replace(args.output)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        print(f"Wrote {args.output} ({len(rendered.encode('utf-8'))} bytes).")
        return 0
    except (NoticeError, OSError) as error:
        print(f"third-party notice error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
