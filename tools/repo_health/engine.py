"""Analysis engine: build the context, run analyzers, assemble the report."""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import List, Optional

from tools.repo_health import __version__
from tools.repo_health.analyzers import Analyzer, default_analyzers
from tools.repo_health.core import CategoryReport, Finding, HealthReport, Severity
from tools.repo_health.discovery import build_context


def run_analysis(root: Path, analyzers: Optional[List[Analyzer]] = None) -> HealthReport:
    """Analyze the repository at ``root`` and return a :class:`HealthReport`.

    Never raises on analyzer failure: a crashing analyzer is downgraded to a
    ``CRITICAL`` finding in its own category so the rest of the report survives.
    """
    analyzers = analyzers if analyzers is not None else default_analyzers()
    ctx = build_context(root)

    report = HealthReport(
        root=str(root),
        version=__version__,
        generated_at=_dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
    )
    for analyzer in analyzers:
        try:
            report.categories.append(analyzer.analyze(ctx))
        except Exception as exc:  # noqa: BLE001 - resilience is the whole point
            cat = CategoryReport(key=analyzer.key, title=analyzer.title)
            cat.add(
                Finding(
                    category=analyzer.key,
                    severity=Severity.CRITICAL,
                    title="Analyzer crashed",
                    detail=f"{type(exc).__name__}: {exc}",
                    recommendation="This is a bug in repo-health; please report it.",
                )
            )
            report.categories.append(cat)
    return report
