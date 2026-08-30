from __future__ import annotations

import pytest


def test_supported_wandb_sdk_exposes_importer_surface() -> None:
    wandb = pytest.importorskip("wandb")
    public = pytest.importorskip("wandb.apis.public")

    assert callable(wandb.Api)
    for method in ("scan_history", "file", "files", "logged_artifacts"):
        assert callable(getattr(public.Run, method, None)), method
