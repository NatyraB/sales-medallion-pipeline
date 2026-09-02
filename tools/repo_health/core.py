"""Core data model and scoring for repo-health.

Everything here is plain-stdlib and side-effect free so it is trivial to unit
test. Scoring is uniform and derived only from findings, which keeps grades
predictable and easy to reason about.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class Severity(enum.IntEnum):
    """Ordered severity levels. Higher is worse."""

    INFO = 0
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4

    @property
    def label(self) -> str:
        return self.name

    @classmethod
    def from_name(cls, name: str) -> "Severity":
        return cls[name.strip().upper()]


# Penalty (in score points) each finding subtracts from its category's 100.
# Tuned so that a few systemic style issues dent the grade without cratering it,
# while correctness/security problems dominate.
_SEVERITY_PENALTY: Dict[Severity, int] = {
    Severity.INFO: 0,
    Severity.LOW: 3,
    Severity.MEDIUM: 9,
    Severity.HIGH: 22,
    Severity.CRITICAL: 45,
}

# Symbols used in the plain-text report (ASCII-safe fallbacks are fine in any
# terminal; these render nicely in modern ones).
SEVERITY_MARK: Dict[Severity, str] = {
    Severity.INFO: "·",
    Severity.LOW: "▪",
    Severity.MEDIUM: "▲",
    Severity.HIGH: "✖",
    Severity.CRITICAL: "‼",
}


@dataclass(frozen=True)
class Finding:
    """A single, actionable observation produced by an analyzer."""

    category: str
    severity: Severity
    title: str
    detail: str = ""
    path: Optional[str] = None
    line: Optional[int] = None
    recommendation: str = ""

    @property
    def location(self) -> str:
        if self.path and self.line:
            return f"{self.path}:{self.line}"
        if self.path:
            return self.path
        return ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "severity": self.severity.label,
            "title": self.title,
            "detail": self.detail,
            "path": self.path,
            "line": self.line,
            "recommendation": self.recommendation,
        }


@dataclass
class CategoryReport:
    """Results for one analyzer category."""

    key: str
    title: str
    findings: List[Finding] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    # Neutral/positive one-liners summarizing what was checked and what looked
    # healthy. These do not affect the score.
    notes: List[str] = field(default_factory=list)

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)

    @property
    def penalty(self) -> int:
        total = sum(_SEVERITY_PENALTY[f.severity] for f in self.findings)
        return min(100, total)

    @property
    def score(self) -> int:
        return max(0, 100 - self.penalty)

    @property
    def grade(self) -> str:
        return grade_for_score(self.score)

    def severity_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {s.label: 0 for s in Severity}
        for f in self.findings:
            counts[f.severity.label] += 1
        return counts

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "score": self.score,
            "grade": self.grade,
            "metrics": self.metrics,
            "notes": list(self.notes),
            "severity_counts": self.severity_counts(),
            "findings": [f.to_dict() for f in self.findings],
        }


@dataclass
class HealthReport:
    """Aggregated report across all categories."""

    root: str
    categories: List[CategoryReport] = field(default_factory=list)
    version: str = ""
    generated_at: str = ""

    @property
    def score(self) -> int:
        if not self.categories:
            return 0
        return round(sum(c.score for c in self.categories) / len(self.categories))

    @property
    def grade(self) -> str:
        return grade_for_score(self.score)

    def all_findings(self) -> List[Finding]:
        out: List[Finding] = []
        for c in self.categories:
            out.extend(c.findings)
        return out

    def top_findings(self, limit: int = 10) -> List[Finding]:
        """Findings ranked most-severe first (stable within a severity)."""
        ranked = sorted(
            self.all_findings(),
            key=lambda f: (-int(f.severity), f.category, f.title),
        )
        return ranked[:limit]

    def severity_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {s.label: 0 for s in Severity}
        for f in self.all_findings():
            counts[f.severity.label] += 1
        return counts

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool": "repo-health",
            "version": self.version,
            "generated_at": self.generated_at,
            "root": self.root,
            "overall": {"score": self.score, "grade": self.grade},
            "severity_counts": self.severity_counts(),
            "categories": [c.to_dict() for c in self.categories],
        }


def grade_for_score(score: int) -> str:
    """Map a 0-100 score to a letter grade."""
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 60:
        return "D"
    return "F"
