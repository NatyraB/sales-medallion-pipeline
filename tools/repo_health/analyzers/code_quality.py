"""Code-quality analyzer: syntax, long files, bare excepts, print-vs-logging,
wildcard imports, TODO/FIXME markers, and unused imports.

Systemic style issues are aggregated into a single finding with a count and a
few example locations, rather than one finding per occurrence. This keeps the
report practical instead of a raw lint dump.
"""

from __future__ import annotations

import ast
import io
import re
import tokenize
from typing import List, Tuple

from tools.repo_health.analyzers.base import Analyzer
from tools.repo_health.core import CategoryReport, Finding, Severity
from tools.repo_health.discovery import AnalysisContext, SourceFile

LONG_FILE_LINES = 400
TODO_RE = re.compile(r"\b(TODO|FIXME|XXX|HACK)\b")
_MAX_EXAMPLES = 4


def _examples(locations: List[str]) -> str:
    shown = locations[:_MAX_EXAMPLES]
    more = len(locations) - len(shown)
    text = ", ".join(shown)
    if more > 0:
        text += f", +{more} more"
    return text


def _bare_excepts(sf: SourceFile) -> List[int]:
    lines: List[int] = []
    for node in ast.walk(sf.tree):
        if isinstance(node, ast.ExceptHandler) and node.type is None:
            lines.append(node.lineno)
    return lines


def _wildcard_imports(sf: SourceFile) -> List[int]:
    lines: List[int] = []
    for node in ast.walk(sf.tree):
        if isinstance(node, ast.ImportFrom):
            if any(alias.name == "*" for alias in node.names):
                lines.append(node.lineno)
    return lines


def _has_print(sf: SourceFile) -> bool:
    for node in ast.walk(sf.tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "print"
        ):
            return True
    return False


def _todo_markers(sf: SourceFile) -> List[int]:
    """Find TODO/FIXME markers inside comments (not string contents)."""
    lines: List[int] = []
    try:
        tokens = tokenize.generate_tokens(io.StringIO(sf.text).readline)
        for tok in tokens:
            if tok.type == tokenize.COMMENT and TODO_RE.search(tok.string):
                lines.append(tok.start[0])
    except (tokenize.TokenError, IndentationError, SyntaxError):
        # Tokenizer can choke on the same files that failed to parse; the
        # syntax-error finding already covers those.
        pass
    return lines


def _unused_imports(sf: SourceFile) -> List[Tuple[str, int]]:
    """Best-effort unused-import detection.

    A bound import name is considered unused when it never appears as a loaded
    ``Name`` anywhere else in the module. Wildcard imports and ``__future__``
    are ignored. This is conservative but catches genuinely dead imports such
    as an unused ``from datetime import datetime``.
    """
    bound: List[Tuple[str, int]] = []  # (used_name, lineno)
    for node in ast.walk(sf.tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.asname or alias.name.split(".")[0]
                bound.append((name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.module == "__future__":
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                name = alias.asname or alias.name
                bound.append((name, node.lineno))

    if not bound:
        return []

    used: set = set()
    for node in ast.walk(sf.tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            used.add(node.id)
        elif isinstance(node, ast.Attribute):
            # Root of an attribute chain, e.g. `F` in `F.col(...)`.
            base = node
            while isinstance(base, ast.Attribute):
                base = base.value
            if isinstance(base, ast.Name):
                used.add(base.id)

    unused = [(name, line) for name, line in bound if name not in used]
    # Names bound more than once (odd, but possible) — de-duplicate by name.
    seen: set = set()
    deduped: List[Tuple[str, int]] = []
    for name, line in unused:
        if name not in seen:
            seen.add(name)
            deduped.append((name, line))
    return deduped


class CodeQualityAnalyzer(Analyzer):
    key = "code_quality"
    title = "Code Quality"

    def analyze(self, ctx: AnalysisContext) -> CategoryReport:
        report = self._report()
        files = ctx.source_layer_files()

        total_lines = 0
        syntax_errors = 0
        bare_locs: List[str] = []
        wildcard_locs: List[str] = []
        print_files: List[str] = []
        todo_locs: List[str] = []
        unused_locs: List[str] = []
        long_files: List[Tuple[str, int]] = []

        for sf in files:
            total_lines += sf.lines

            if sf.decode_error:
                report.add(
                    Finding(
                        category=self.key,
                        severity=Severity.MEDIUM,
                        title="File could not be decoded as UTF-8",
                        detail=f"{sf.rel} was skipped for AST analysis.",
                        path=sf.rel,
                        recommendation="Re-save the file as UTF-8.",
                    )
                )
                continue

            if sf.parse_error is not None:
                syntax_errors += 1
                report.add(
                    Finding(
                        category=self.key,
                        severity=Severity.HIGH,
                        title="Python syntax error",
                        detail=f"{sf.rel}: {sf.parse_error}",
                        path=sf.rel,
                        line=sf.parse_error_line,
                        recommendation="Fix the syntax error so the file can run and be analyzed.",
                    )
                )
                continue  # Can't AST-walk a file that didn't parse.

            if sf.lines > LONG_FILE_LINES:
                long_files.append((sf.rel, sf.lines))

            for line in _bare_excepts(sf):
                bare_locs.append(f"{sf.rel}:{line}")
            for line in _wildcard_imports(sf):
                wildcard_locs.append(f"{sf.rel}:{line}")
            if _has_print(sf):
                print_files.append(sf.rel)
            for line in _todo_markers(sf):
                todo_locs.append(f"{sf.rel}:{line}")
            for name, line in _unused_imports(sf):
                unused_locs.append(f"{sf.rel}:{line} ({name})")

        for rel, lines in sorted(long_files, key=lambda x: -x[1]):
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.MEDIUM,
                    title=f"Very long file ({lines} lines)",
                    detail=f"{rel} exceeds {LONG_FILE_LINES} lines.",
                    path=rel,
                    recommendation="Split into smaller, focused modules or notebook steps.",
                )
            )

        if bare_locs:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.LOW,
                    title=f"Bare `except:` clauses ({len(bare_locs)})",
                    detail=f"Silent catch-all handlers hide errors. Locations: {_examples(bare_locs)}.",
                    recommendation="Catch a specific exception (e.g. `except Exception as exc:`) and log it.",
                )
            )

        if wildcard_locs:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.LOW,
                    title=f"Wildcard imports ({len(wildcard_locs)})",
                    detail=(
                        "`from pyspark.sql.types import *` pollutes the namespace and hides "
                        f"unused names. Locations: {_examples(wildcard_locs)}."
                    ),
                    recommendation="Import only the types you use (e.g. `StructType, StructField, StringType`).",
                )
            )

        if print_files:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.LOW,
                    title=f"`print()` used for output in {len(print_files)} files",
                    detail=(
                        "Pipeline steps rely on `print()` rather than structured logging. "
                        f"Files: {_examples(print_files)}."
                    ),
                    recommendation=(
                        "Prefer the `logging` module so job output has levels and timestamps "
                        "(acceptable in exploratory notebook cells, but not for status/errors)."
                    ),
                )
            )

        if unused_locs:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.LOW,
                    title=f"Unused imports ({len(unused_locs)})",
                    detail=f"Imported but never referenced. Locations: {_examples(unused_locs)}.",
                    recommendation="Remove the unused imports.",
                )
            )

        if todo_locs:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.INFO,
                    title=f"TODO/FIXME markers ({len(todo_locs)})",
                    detail=f"Unresolved markers left in code. Locations: {_examples(todo_locs)}.",
                    recommendation="Track these as issues or resolve them.",
                )
            )

        report.metrics = {
            "files_analyzed": len(files),
            "total_lines": total_lines,
            "syntax_errors": syntax_errors,
            "bare_excepts": len(bare_locs),
            "wildcard_imports": len(wildcard_locs),
            "print_files": len(print_files),
            "unused_imports": len(unused_locs),
            "todo_markers": len(todo_locs),
            "long_files": len(long_files),
        }
        if syntax_errors == 0:
            report.notes.append(f"All {len(files)} source files parse cleanly (no syntax errors).")
        return report
