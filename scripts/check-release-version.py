#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parent.parent
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
SEMVER_PRERELEASE = re.compile(
    r"^(?P<base>\d+\.\d+\.\d+)-(?P<kind>alpha|beta|rc)\.(?P<number>\d+)$"
)


def load_toml(path: Path) -> dict[str, object]:
    with path.open("rb") as source:
        return tomllib.load(source)


def python_version_from_source(path: Path) -> str:
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for statement in module.body:
        if (
            isinstance(statement, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__version__"
                for target in statement.targets
            )
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        ):
            return statement.value.value
    raise ValueError(f"{path.relative_to(ROOT)} has no literal __version__ assignment")


def pep440_version(semver: str) -> str:
    if SEMVER.fullmatch(semver):
        return semver
    match = SEMVER_PRERELEASE.fullmatch(semver)
    if match is None:
        raise ValueError(f"unsupported release version: {semver!r}")
    marker = {"alpha": "a", "beta": "b", "rc": "rc"}[match.group("kind")]
    return f"{match.group('base')}{marker}{match.group('number')}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check EpochDeck release version consistency."
    )
    parser.add_argument("--tag", help="Optional Git tag, including its leading v.")
    parser.add_argument(
        "--require-prerelease",
        action="store_true",
        help="Reject a stable version in the prerelease workflow.",
    )
    parser.add_argument(
        "--require-release-ready",
        action="store_true",
        help="Require resolved release metadata, licensing, and changelog state.",
    )
    arguments = parser.parse_args()

    cargo = load_toml(ROOT / "Cargo.toml")
    cargo_lock = load_toml(ROOT / "Cargo.lock")
    python_project = load_toml(ROOT / "python" / "pyproject.toml")
    python_lock = load_toml(ROOT / "python" / "uv.lock")
    web = json.loads((ROOT / "web" / "package.json").read_text(encoding="utf-8"))

    release_version = cargo["workspace"]["package"]["version"]
    if not isinstance(release_version, str):
        raise TypeError("Cargo workspace version must be a string")
    expected_python = pep440_version(release_version)
    actual_python = python_project["project"]["version"]
    source_python = python_version_from_source(
        ROOT / "python" / "src" / "epochdeck" / "__init__.py"
    )
    failures = []
    workspace_names = set()
    for member in cargo["workspace"]["members"]:
        manifest = load_toml(ROOT / member / "Cargo.toml")
        workspace_names.add(manifest["package"]["name"])
    locked_workspace_versions = {
        package["name"]: package["version"]
        for package in cargo_lock["package"]
        if package.get("name") in workspace_names
    }
    for package_name in sorted(workspace_names):
        locked_version = locked_workspace_versions.get(package_name)
        if locked_version != release_version:
            failures.append(
                f"Cargo.lock {package_name} version is {locked_version!r}, "
                f"expected {release_version!r}"
            )

    distribution_name = python_project["project"]["name"]
    editable_packages = [
        package
        for package in python_lock["package"]
        if package.get("source") == {"editable": "."}
    ]
    if len(editable_packages) != 1:
        failures.append(
            f"python/uv.lock has {len(editable_packages)} editable project entries, expected one"
        )
    else:
        locked_python = editable_packages[0]
        if locked_python.get("name") != distribution_name:
            failures.append(
                f"python/uv.lock project name is {locked_python.get('name')!r}, "
                f"expected {distribution_name!r}"
            )
        if locked_python.get("version") != expected_python:
            failures.append(
                f"python/uv.lock project version is {locked_python.get('version')!r}, "
                f"expected {expected_python!r}"
            )

    if web.get("version") != release_version:
        failures.append(
            f"web version is {web.get('version')!r}, expected {release_version!r}"
        )
    if actual_python != expected_python:
        failures.append(
            f"Python package version is {actual_python!r}, expected {expected_python!r}"
        )
    if source_python != expected_python:
        failures.append(
            f"epochdeck.__version__ is {source_python!r}, expected {expected_python!r}"
        )
    if arguments.tag is not None and arguments.tag != f"v{release_version}":
        failures.append(f"tag is {arguments.tag!r}, expected 'v{release_version}'")
    if (
        arguments.require_prerelease
        and SEMVER_PRERELEASE.fullmatch(release_version) is None
    ):
        failures.append(f"version {release_version!r} is not a prerelease")
    release_notes_tag = arguments.tag
    if arguments.require_release_ready and release_notes_tag is None:
        release_notes_tag = f"v{release_version}"
    if release_notes_tag is not None:
        notes = ROOT / "docs" / "releases" / f"{release_notes_tag}.md"
        if not notes.is_file():
            failures.append(f"release notes are missing: {notes.relative_to(ROOT)}")
    if arguments.require_release_ready:
        cargo_license = cargo["workspace"]["package"].get("license")
        python_license = python_project["project"].get("license")
        root_license = ROOT / "LICENSE"
        packaged_python_license = ROOT / "python" / "LICENSE"
        if not root_license.is_file():
            failures.append("LICENSE is missing")
        if not packaged_python_license.is_file():
            failures.append("python/LICENSE is missing")
        if (
            root_license.is_file()
            and packaged_python_license.is_file()
            and root_license.read_bytes() != packaged_python_license.read_bytes()
        ):
            failures.append("python/LICENSE does not match the root LICENSE")
        if not isinstance(cargo_license, str) or not cargo_license.strip():
            failures.append("Cargo workspace license metadata is unresolved")
        if python_license != cargo_license:
            failures.append(
                f"Python license is {python_license!r}, expected Cargo license {cargo_license!r}"
            )
        if python_project["project"].get("license-files") != ["LICENSE"]:
            failures.append("Python license-files must be exactly ['LICENSE']")
        for member in cargo["workspace"]["members"]:
            package = load_toml(ROOT / member / "Cargo.toml")["package"]
            if package.get("license") != {"workspace": True}:
                failures.append(
                    f"{member}/Cargo.toml must inherit the workspace license"
                )
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        if "distribution filename will be finalized" in readme:
            failures.append(
                "Python distribution identity is still marked unresolved in README.md"
            )
        if "License is intentionally undecided" in readme:
            failures.append("project license is still marked unresolved in README.md")
        changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        release_heading = re.compile(
            rf"^## \[{re.escape(release_version)}\] - \d{{4}}-\d{{2}}-\d{{2}}$",
            re.MULTILINE,
        )
        if release_heading.search(changelog) is None:
            failures.append(
                f"CHANGELOG.md needs a dated [{release_version}] release heading"
            )
    if failures:
        for failure in failures:
            print(f"release version error: {failure}", file=sys.stderr)
        return 1
    print(f"release versions agree on v{release_version} / Python {expected_python}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
