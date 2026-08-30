from pathlib import Path

import pytest

from epochdeck import Artifact, Image


def test_rich_media_rejects_an_unsafe_source_basename_before_spooling(tmp_path: Path) -> None:
    source = tmp_path / "nested\\frame.png"
    source.write_bytes(b"frame")

    with pytest.raises(ValueError, match="file_name"):
        Image(source)._prepare(tmp_path / "rich-blobs")


def test_artifact_rejects_an_unsafe_source_basename_before_spooling(tmp_path: Path) -> None:
    source = tmp_path / "nested\\checkpoint.bin"
    source.write_bytes(b"checkpoint")
    artifact = Artifact("policy", type="model").add_file(source, name="checkpoint.bin")

    with pytest.raises(ValueError, match="file_name"):
        artifact._prepare(tmp_path / "artifact-blobs", aliases=[])
