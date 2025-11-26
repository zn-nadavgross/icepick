#!/usr/bin/env python3
"""
Basic static checks for Rust sources.

The script guards two simple heuristics:
1. Lines of code per file (ignoring blank and comment-only lines)
2. A rough cyclomatic complexity proxy based on control-flow keywords

Thresholds can be changed via CLI flags or environment variables:
- MAX_LOC_PER_FILE (default: 400)
- MAX_COMPLEXITY_SCORE (default: 60)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Iterable, Tuple


DEFAULT_MAX_LOC = int(os.getenv("MAX_LOC_PER_FILE", "450"))
DEFAULT_MAX_COMPLEXITY = int(os.getenv("MAX_COMPLEXITY_SCORE", "60"))

# Regex fragments roughly representing cyclomatic complexity contributors.
COMPLEXITY_PATTERNS: Tuple[Tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"\bif\b"), 1),
    (re.compile(r"\belse\s+if\b"), 1),
    (re.compile(r"\bmatch\b"), 1),
    (re.compile(r"\bfor\b"), 1),
    (re.compile(r"\bwhile\b"), 1),
    (re.compile(r"\bloop\b"), 1),
    (re.compile(r"\?"), 1),  # the try operator usually indicates branching
    (re.compile(r"&&|\|\|"), 1),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail if Rust files grow beyond allowable LOC or complexity."
    )
    parser.add_argument(
        "--max-loc",
        type=int,
        default=DEFAULT_MAX_LOC,
        help=f"Maximum non-comment LOC per file (default: {DEFAULT_MAX_LOC})",
    )
    parser.add_argument(
        "--max-complexity",
        type=int,
        default=DEFAULT_MAX_COMPLEXITY,
        help=(
            "Maximum heuristic complexity score per file "
            f"(default: {DEFAULT_MAX_COMPLEXITY})"
        ),
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Optional explicit list of Rust source files or directories to scan.",
    )
    return parser.parse_args()


def rust_files_from_args(provided_paths: Iterable[Path]) -> Iterable[Path]:
    if provided_paths:
        for path in provided_paths:
            if path.is_file() and path.suffix == ".rs":
                yield path
            elif path.is_dir():
                yield from path.rglob("*.rs")
        return

    search_roots = ("src", "tests", "examples", "benches")
    for root in search_roots:
        root_path = Path(root)
        if not root_path.exists():
            continue
        yield from root_path.rglob("*.rs")


def strip_comments(line: str, in_block: bool) -> Tuple[str, bool]:
    """
    Remove Rust line/block comments while tracking block comment state.
    Returns the processed line and whether we remain inside a block comment.
    """
    i = 0
    cleaned = []
    while i < len(line):
        if in_block:
            end = line.find("*/", i)
            if end == -1:
                return "", True
            i = end + 2
            in_block = False
            continue
        block_start = line.find("/*", i)
        line_comment = line.find("//", i)
        if line_comment != -1 and (block_start == -1 or line_comment < block_start):
            cleaned.append(line[i:line_comment])
            break
        if block_start == -1:
            cleaned.append(line[i:])
            break
        cleaned.append(line[i:block_start])
        i = block_start + 2
        in_block = True
    return "".join(cleaned), in_block


def metrics_for_file(path: Path) -> Tuple[int, int]:
    """
    Returns a tuple (loc, complexity_score) for the given Rust file.
    - LOC counts non-empty, non-comment lines.
    - Complexity sums the occurrences of heuristically interesting tokens.
    """
    loc = 0
    complexity = 0
    in_block_comment = False

    text = path.read_text(encoding="utf-8")
    for raw_line in text.splitlines():
        processed_line, in_block_comment = strip_comments(raw_line, in_block_comment)
        stripped = processed_line.strip()
        if not stripped:
            continue
        loc += 1
        for pattern, weight in COMPLEXITY_PATTERNS:
            complexity += weight * len(pattern.findall(stripped))

    return loc, complexity


def main() -> int:
    args = parse_args()
    failures: list[str] = []

    for file_path in sorted(set(rust_files_from_args(args.paths))):
        loc, complexity = metrics_for_file(file_path)
        reasons = []
        if loc > args.max_loc:
            reasons.append(f"LOC {loc} > allowed {args.max_loc}")
        if complexity > args.max_complexity:
            reasons.append(
                f"complexity {complexity} > allowed {args.max_complexity}"
            )
        if reasons:
            failures.append(f"{file_path}: {', '.join(reasons)}")

    if failures:
        print("Rust quality thresholds exceeded:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print(
            "Adjust the code or override the limits via MAX_LOC_PER_FILE / "
            "MAX_COMPLEXITY_SCORE.",
            file=sys.stderr,
        )
        return 1

    print("Rust quality thresholds look good.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
