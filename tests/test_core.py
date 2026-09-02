"""Tests for the scoring/grading data model."""

from tools.repo_health.core import (
    CategoryReport,
    Finding,
    HealthReport,
    Severity,
    grade_for_score,
)


def _finding(sev: Severity, title: str = "x") -> Finding:
    return Finding(category="c", severity=sev, title=title)


def test_grade_boundaries():
    assert grade_for_score(100) == "A"
    assert grade_for_score(90) == "A"
    assert grade_for_score(89) == "B"
    assert grade_for_score(80) == "B"
    assert grade_for_score(70) == "C"
    assert grade_for_score(60) == "D"
    assert grade_for_score(59) == "F"
    assert grade_for_score(0) == "F"


def test_category_penalty_and_score():
    cat = CategoryReport(key="c", title="C")
    assert cat.score == 100  # no findings
    cat.add(_finding(Severity.LOW))       # -3
    cat.add(_finding(Severity.MEDIUM))    # -9
    cat.add(_finding(Severity.INFO))      # -0
    assert cat.penalty == 12
    assert cat.score == 88
    assert cat.grade == "B"


def test_category_score_floored_at_zero():
    cat = CategoryReport(key="c", title="C")
    for _ in range(3):
        cat.add(_finding(Severity.CRITICAL))  # 45 * 3 = 135, capped at 100
    assert cat.penalty == 100
    assert cat.score == 0
    assert cat.grade == "F"


def test_overall_score_is_category_average():
    c1 = CategoryReport(key="a", title="A")           # 100
    c2 = CategoryReport(key="b", title="B")
    c2.add(_finding(Severity.HIGH))                    # 78
    report = HealthReport(root="/tmp", categories=[c1, c2])
    assert report.score == round((100 + 78) / 2)
    assert report.grade == grade_for_score(report.score)


def test_top_findings_ranked_most_severe_first():
    cat = CategoryReport(key="c", title="C")
    cat.add(_finding(Severity.LOW, "low"))
    cat.add(_finding(Severity.CRITICAL, "crit"))
    cat.add(_finding(Severity.MEDIUM, "med"))
    report = HealthReport(root="/tmp", categories=[cat])
    titles = [f.title for f in report.top_findings(limit=10)]
    assert titles == ["crit", "med", "low"]


def test_severity_counts():
    cat = CategoryReport(key="c", title="C")
    cat.add(_finding(Severity.LOW))
    cat.add(_finding(Severity.LOW))
    report = HealthReport(root="/tmp", categories=[cat])
    assert report.severity_counts()["LOW"] == 2
    assert report.severity_counts()["HIGH"] == 0
