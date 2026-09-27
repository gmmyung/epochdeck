#!/usr/bin/env python3
"""Measure bounded tail recovery for a long, wide SDK metric journal."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import tracemalloc
import uuid
import warnings
from pathlib import Path

from epochdeck._spool import _Spool


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rows", type=int, nargs="?", default=200_000)
    parser.add_argument("metrics", type=int, nargs="?", default=180)
    args = parser.parse_args()
    if not 1 <= args.rows <= 10_000_000 or not 1 <= args.metrics <= 1_024:
        parser.error("rows must be 1..10000000 and metrics must be 1..1024")
    target = Path(__file__).resolve().parents[1] / "target"
    target.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="spool-benchmark-", dir=target
    ) as temporary:
        root = Path(temporary)
        run_id = str(uuid.uuid4())
        spool = _Spool(root, run_id)
        journal = spool.events_path
        spool.close()
        values = {
            f"train/metric-{index:03}": index / 100 for index in range(args.metrics)
        }
        # Prepare one bounded record at a time; setup is outside the measured region.
        with journal.open("wb") as stream:
            for sequence in range(1, args.rows + 1):
                record = {
                    "sequence": sequence,
                    "step": sequence - 1,
                    "timestamp_ms": sequence,
                    "metrics": values,
                }
                stream.write(json.dumps(record, separators=(",", ":")).encode() + b"\n")
            complete_size = stream.tell()
            stream.write(b'{"sequence":' + b" " * 65_536)
            stream.flush()
            os.fsync(stream.fileno())
        tracemalloc.start()
        started = time.perf_counter()
        with warnings.catch_warnings(record=True) as recovered_warnings:
            warnings.simplefilter("always")
            recovered = _Spool(root, run_id)
        try:
            point = recovered.last_point()
            assert point is not None and point["sequence"] == args.rows
            assert journal.stat().st_size == complete_size
            assert len(recovered_warnings) == 1
            elapsed = time.perf_counter() - started
            _, peak = tracemalloc.get_traced_memory()
        finally:
            recovered.close()
            tracemalloc.stop()
        print(
            f"rows={args.rows} metrics={args.metrics} journal_mib={complete_size / 2**20:.2f} "
            f"recovery_seconds={elapsed:.6f} peak_python_mib={peak / 2**20:.2f}"
        )


if __name__ == "__main__":
    main()
