#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["resvg-py==0.5.0", "pillow==12.2.0"]
# ///
"""Derive browser icons from web/public/epochdeck-mark.svg.

Run with `nix develop --command just brand-assets` after editing the SVG master.
Raster icons use four-times supersampling for smooth edges at small sizes.
These tools are build-only and are not dependencies of the EpochDeck SDK.
"""

from __future__ import annotations

import io
import re
from pathlib import Path

import resvg_py
from PIL import Image

PUBLIC = Path(__file__).resolve().parents[1] / "web" / "public"


def render(svg: str, size: int) -> Image.Image:
    encoded = resvg_py.svg_to_bytes(
        svg_string=svg, width=size * 4, height=size * 4, skip_system_fonts=True
    )
    with Image.open(io.BytesIO(encoded)) as source:
        return source.convert("RGBA").resize((size, size), Image.Resampling.LANCZOS)


def main() -> None:
    svg = (PUBLIC / "epochdeck-mark.svg").read_text(encoding="utf-8")
    (PUBLIC / "favicon.svg").write_text(svg, encoding="utf-8")
    monochrome = re.sub(r"\s*<defs>.*?</defs>", "", svg, flags=re.DOTALL)
    monochrome = monochrome.replace("url(#epochdeck-blue)", "#000")
    (PUBLIC / "safari-pinned-tab.svg").write_text(monochrome, encoding="utf-8")
    render(svg, 32).save(PUBLIC / "favicon-32x32.png", optimize=True)
    render(svg, 180).save(PUBLIC / "apple-touch-icon.png", optimize=True)
    icons = [render(svg, size) for size in (16, 32, 48)]
    icons[-1].save(
        PUBLIC / "favicon.ico",
        format="ICO",
        sizes=[(16, 16), (32, 32), (48, 48)],
        append_images=icons[:-1],
    )


if __name__ == "__main__":
    main()
