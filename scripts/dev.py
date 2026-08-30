"""Run the API and dashboard development servers as one supervised process."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass

SHUTDOWN_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class Child:
    name: str
    process: subprocess.Popen[bytes]


def start_child(name: str, command: Sequence[str]) -> Child:
    process = subprocess.Popen(command, start_new_session=True)
    return Child(name=name, process=process)


def signal_child(child: Child, signal_number: int) -> None:
    if child.process.poll() is not None:
        return
    try:
        os.killpg(child.process.pid, signal_number)
    except ProcessLookupError:
        pass


def stop_children(children: Sequence[Child], signal_number: int) -> None:
    for child in children:
        signal_child(child, signal_number)

    deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if all(child.process.poll() is not None for child in children):
            break
        time.sleep(0.05)

    for child in children:
        signal_child(child, signal.SIGKILL)
    for child in children:
        child.process.wait()


def exit_status(return_code: int) -> int:
    return return_code if return_code >= 0 else 128 - return_code


def main() -> int:
    if os.name != "posix":
        print("the development supervisor requires a POSIX host", file=sys.stderr)
        return 2

    received_signal: list[int] = []

    def request_shutdown(signal_number: int, _frame: object) -> None:
        if not received_signal:
            received_signal.append(signal_number)

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    children: list[Child] = []
    try:
        children.append(start_child("API", ("cargo", "run", "-p", "epochdeck-server")))
        children.append(start_child("dashboard", ("pnpm", "--dir", "web", "dev")))

        while not received_signal:
            exited = next(
                (child for child in children if child.process.poll() is not None),
                None,
            )
            if exited is not None:
                assert exited.process.returncode is not None
                status = exit_status(exited.process.returncode)
                print(
                    f"{exited.name} development process exited with status {status}; "
                    "stopping its sibling",
                    file=sys.stderr,
                )
                stop_children(children, signal.SIGTERM)
                return status
            time.sleep(0.1)

        signal_number = received_signal[0]
        stop_children(children, signal_number)
        return 128 + signal_number
    except BaseException:
        stop_children(children, signal.SIGTERM)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
