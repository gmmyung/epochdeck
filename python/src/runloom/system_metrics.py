from __future__ import annotations

import os
import shutil
import subprocess
import threading
import warnings
from collections.abc import Callable, Mapping
from pathlib import Path

import psutil

_MAX_GPUS = 16
_GPU_QUERY_TIMEOUT_SECONDS = 2.0


class SystemSampler:
    def __init__(self, disk_path: Path) -> None:
        self._disk_path = _existing_parent(disk_path)
        self._process = psutil.Process()
        self._nvidia_smi = shutil.which("nvidia-smi")
        psutil.cpu_percent(interval=None)
        self._process.cpu_percent(interval=None)

    def sample(self) -> dict[str, float]:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage(str(self._disk_path))
        network = psutil.net_io_counters()
        process_memory = self._process.memory_info()
        metrics = {
            "system/cpu_percent": float(psutil.cpu_percent(interval=None)),
            "system/memory_percent": float(memory.percent),
            "system/memory_used_bytes": float(memory.used),
            "system/disk_percent": float(disk.percent),
            "system/disk_used_bytes": float(disk.used),
            "system/network_sent_bytes": float(network.bytes_sent),
            "system/network_received_bytes": float(network.bytes_recv),
            "system/process_cpu_percent": float(self._process.cpu_percent(interval=None)),
            "system/process_rss_bytes": float(process_memory.rss),
        }
        if hasattr(os, "getloadavg"):
            load_1m, load_5m, load_15m = os.getloadavg()
            metrics.update(
                {
                    "system/load_1m": float(load_1m),
                    "system/load_5m": float(load_5m),
                    "system/load_15m": float(load_15m),
                }
            )
        metrics.update(self._gpu_metrics())
        return metrics

    def _gpu_metrics(self) -> dict[str, float]:
        if self._nvidia_smi is None:
            return {}
        try:
            result = subprocess.run(
                [
                    self._nvidia_smi,
                    "--query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu",
                    "--format=csv,noheader,nounits",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=_GPU_QUERY_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired):
            self._nvidia_smi = None
            return {}
        if result.returncode != 0:
            return {}
        metrics: dict[str, float] = {}
        for index, line in enumerate(result.stdout.splitlines()[:_MAX_GPUS]):
            fields = [field.strip() for field in line.split(",")]
            if len(fields) != 4:
                continue
            try:
                utilization, memory_used_mib, memory_total_mib, temperature = map(float, fields)
            except ValueError:
                continue
            prefix = f"system/gpu/{index}"
            metrics[f"{prefix}/utilization_percent"] = utilization
            metrics[f"{prefix}/memory_used_bytes"] = memory_used_mib * 1024 * 1024
            metrics[f"{prefix}/memory_total_bytes"] = memory_total_mib * 1024 * 1024
            metrics[f"{prefix}/temperature_celsius"] = temperature
        return metrics


class SystemMonitor(threading.Thread):
    def __init__(
        self,
        *,
        interval: float,
        sampler: Callable[[], Mapping[str, float]],
        recorder: Callable[[Mapping[str, float]], None],
    ) -> None:
        super().__init__(name="runloom-system-metrics", daemon=True)
        if interval <= 0:
            raise ValueError("system metric interval must be positive")
        self._interval = interval
        self._sampler = sampler
        self._recorder = recorder
        self._stopping = threading.Event()
        self.last_error: Exception | None = None

    def stop(self) -> None:
        self._stopping.set()

    def run(self) -> None:
        warned = False
        while not self._stopping.wait(self._interval):
            try:
                self._recorder(self._sampler())
                self.last_error = None
            except Exception as error:
                self.last_error = error
                if not warned:
                    warnings.warn(
                        f"Runloom system metrics encountered an error: {error}",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                    warned = True


def _existing_parent(path: Path) -> Path:
    candidate = path.resolve()
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            return Path.cwd()
        candidate = parent
    return candidate
