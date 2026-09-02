"""Security analyzer.

Scans source and config text for hardcoded secrets, embedded workspace
hostnames/URLs, and a few unsafe patterns. Environment-variable and secret-
manager usage (``os.environ``, ``dbutils.secrets``, ``${{ secrets.* }}``) is
treated as the *correct* pattern, not a finding.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Tuple

from tools.repo_health.analyzers.base import Analyzer
from tools.repo_health.core import CategoryReport, Finding, Severity
from tools.repo_health.discovery import AnalysisContext

# Substrings that indicate a value is safely sourced from the environment / a
# secret manager rather than hardcoded.
_ENV_REFERENCE = ("os.environ", "os.getenv", "getenv(", "dbutils.secrets", "${{", "${", "{{")

# Obvious non-secret placeholder values.
_PLACEHOLDERS = {"changeme", "your_token", "xxx", "todo", "none", "null", "example"}

# High-signal secret patterns (independent of variable names).
_TOKEN_PATTERNS: List[Tuple[str, "re.Pattern[str]"]] = [
    ("AWS access key id", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("Private key block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("GitHub token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b")),
    ("Slack token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
]

# Assignment of a secret-ish key to a quoted string literal.
_ASSIGN_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|client[_-]?secret)\b"
    r"\s*[:=]\s*['\"]([^'\"]+)['\"]"
)

# Workspace host / URL patterns.
_URL_PATTERNS: List["re.Pattern[str]"] = [
    re.compile(r"https?://[^\s'\"]*\.cloud\.databricks\.com[^\s'\"]*"),
    re.compile(r"\b[a-z0-9-]+\.cloud\.databricks\.com\b"),
]


def _looks_like_real_secret(value: str) -> bool:
    v = value.strip()
    if len(v) < 6:
        return False
    if v.lower() in _PLACEHOLDERS:
        return False
    if any(ref in v for ref in _ENV_REFERENCE):
        return False
    return True


def _iter_lines(ctx: AnalysisContext) -> Iterable[Tuple[str, int, str]]:
    for sf in ctx.source_files:
        if sf.decode_error:
            continue
        for i, line in enumerate(sf.text.splitlines(), start=1):
            yield sf.rel, i, line
    for tf in ctx.text_files:
        for i, line in enumerate(tf.text.splitlines(), start=1):
            yield tf.rel, i, line


class SecurityAnalyzer(Analyzer):
    key = "security"
    title = "Security"

    def analyze(self, ctx: AnalysisContext) -> CategoryReport:
        report = self._report()

        secret_hits: List[str] = []
        url_hits: List[str] = []
        env_usage = 0
        files_scanned = len({rel for rel, _, _ in _iter_lines(ctx)})

        for rel, lineno, line in _iter_lines(ctx):
            if any(ref in line for ref in _ENV_REFERENCE):
                env_usage += 1

            for label, pattern in _TOKEN_PATTERNS:
                if pattern.search(line):
                    secret_hits.append(f"{rel}:{lineno} ({label})")

            assign = _ASSIGN_RE.search(line)
            if assign and _looks_like_real_secret(assign.group(2)):
                secret_hits.append(f"{rel}:{lineno} (hardcoded {assign.group(1).lower()})")

            for pattern in _URL_PATTERNS:
                if pattern.search(line):
                    url_hits.append(f"{rel}:{lineno}")
                    break

        if secret_hits:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.CRITICAL,
                    title=f"Possible hardcoded secrets ({len(secret_hits)})",
                    detail="Credential-like literals found: " + ", ".join(sorted(secret_hits)),
                    recommendation="Move secrets to environment variables or a secret manager and rotate them.",
                )
            )

        if url_hits:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.LOW,
                    title=f"Embedded workspace host URL(s) ({len(url_hits)})",
                    detail=(
                        "Databricks workspace hostnames are hardcoded (typically in bundle config): "
                        + ", ".join(sorted(set(url_hits)))
                        + "."
                    ),
                    recommendation=(
                        "Parameterize the host via bundle variables / CI secrets so the same code "
                        "targets multiple workspaces without edits."
                    ),
                )
            )

        report.metrics = {
            "files_scanned": files_scanned,
            "secret_hits": len(secret_hits),
            "url_hits": len(set(url_hits)),
            "env_reference_lines": env_usage,
        }
        if not secret_hits:
            report.notes.append("No hardcoded credentials detected in source or config.")
        if env_usage:
            report.notes.append(
                f"Environment/secret-manager references found on {env_usage} line(s) — good practice."
            )
        return report
