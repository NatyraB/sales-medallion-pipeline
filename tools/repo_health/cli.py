"""Command-line interface for repo-health.

Run with ``python -m tools.repo_health`` (optionally ``--json``).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

from tools.repo_health import __version__
from tools.repo_health.core import Severity
from tools.repo_health.discovery import find_repo_root
from tools.repo_health.engine import run_analysis
from tools.repo_health.report import render_json, render_text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="repo-health",
        description="Print a concise, prioritized health report for this repository.",
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=None,
        help="Repository root to analyze (default: auto-detect from the current directory).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a machine-readable JSON report instead of the text summary.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of top findings to show in the text summary (default: 10).",
    )
    parser.add_argument(
        "--min-severity",
        choices=[s.label for s in Severity],
        default="INFO",
        help="Hide findings below this severity in the text summary (default: INFO).",
    )
    parser.add_argument(
        "--fail-under",
        type=int,
        default=None,
        metavar="SCORE",
        help="Exit non-zero if the overall score is below SCORE (for CI gating). Off by default.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"repo-health {__version__}",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    root = Path(args.path).resolve() if args.path else find_repo_root()
    if not root.is_dir():
        print(f"error: not a directory: {root}", file=sys.stderr)
        return 2

    report = run_analysis(root)

    if args.json:
        print(render_json(report))
    else:
        print(
            render_text(
                report,
                top=args.top,
                min_severity=Severity.from_name(args.min_severity),
            )
        )

    if args.fail_under is not None and report.score < args.fail_under:
        print(
            f"\nrepo-health: score {report.score} is below --fail-under {args.fail_under}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
