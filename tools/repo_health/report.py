"""Rendering: concise ranked text summary and machine-readable JSON."""

from __future__ import annotations

import json
from typing import List

from tools.repo_health.core import (
    SEVERITY_MARK,
    Finding,
    HealthReport,
    Severity,
)

_BAR_WIDTH = 20


def render_json(report: HealthReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=False)


def _score_bar(score: int) -> str:
    filled = round(score / 100 * _BAR_WIDTH)
    return "█" * filled + "░" * (_BAR_WIDTH - filled)


def _wrap_reco(text: str, indent: str) -> str:
    return f"{indent}↳ {text}"


def render_text(report: HealthReport, top: int = 10, min_severity: Severity = Severity.INFO) -> str:
    lines: List[str] = []
    add = lines.append

    add("=" * 68)
    add("  REPOSITORY HEALTH REPORT")
    add(f"  {report.root}")
    add("=" * 68)
    add("")
    add(f"  OVERALL GRADE: {report.grade}   ({report.score}/100)")
    add(f"  [{_score_bar(report.score)}]")

    counts = report.severity_counts()
    summary_bits = [
        f"{counts[s.label]} {s.label.lower()}"
        for s in (Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW)
        if counts[s.label]
    ]
    add("  Findings: " + (", ".join(summary_bits) if summary_bits else "none"))
    add("")

    # ---- Per-category status ---------------------------------------------
    add("-" * 68)
    add("  CATEGORY STATUS")
    add("-" * 68)
    for cat in report.categories:
        add(f"  {cat.grade}  {cat.title:<34} {cat.score:>3}/100")
        for note in cat.notes:
            add(f"        · {note}")
    add("")

    # ---- Top findings -----------------------------------------------------
    findings = [f for f in report.top_findings(limit=top) if f.severity >= min_severity]
    add("-" * 68)
    add(f"  TOP FINDINGS (most severe first, showing up to {top})")
    add("-" * 68)
    if not findings:
        add("  🎉 No findings at or above the selected severity.")
    for f in findings:
        mark = SEVERITY_MARK[f.severity]
        loc = f" [{f.location}]" if f.location else ""
        add(f"  {mark} {f.severity.label:<8} {f.title}{loc}")
        if f.detail:
            add(f"        {f.detail}")
        if f.recommendation:
            add(_wrap_reco(f.recommendation, "        "))
    add("")

    # ---- Consolidated recommendations ------------------------------------
    recos: List[str] = []
    for f in report.top_findings(limit=max(top, 12)):
        if f.recommendation and f.recommendation not in recos:
            recos.append(f.recommendation)
    if recos:
        add("-" * 68)
        add("  RECOMMENDATIONS")
        add("-" * 68)
        for i, reco in enumerate(recos[:8], start=1):
            add(f"  {i}. {reco}")
        add("")

    add("=" * 68)
    add(f"  repo-health v{report.version} · run with --json for machine-readable output")
    add("=" * 68)
    return "\n".join(lines)
