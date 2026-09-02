"""Repository discovery: locate the repo root and read source files once.

Analyzers operate on an :class:`AnalysisContext` so that every file is read and
AST-parsed exactly once, and so that unit tests can point the analyzers at a
temporary fixture directory instead of the real repo.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

# Directories we never descend into. ``tests`` is excluded from code analysis
# because the dedicated testing analyzer evaluates it separately, and its
# fixtures deliberately contain broken code.
EXCLUDE_DIR_NAMES = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "ENV",
    ".databricks",
    ".bundle",
    "build",
    "dist",
    "node_modules",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".idea",
    ".vscode",
    "tests",
}

# Markers that identify the repository root when walking up from the CWD, in
# decreasing order of specificity. A more specific marker anywhere in the
# ancestry wins over a less specific one nearer the start dir, so a stray
# README.md in a parent directory cannot hijack root detection away from the
# directory that actually holds databricks.yml / .git.
ROOT_MARKERS = ("databricks.yml", ".git", "pyproject.toml", "README.md")

# The tool's own package directory. We never analyze ourselves so the report
# stays focused on the pipeline.
_TOOL_DIR = Path(__file__).resolve().parent


@dataclass
class SourceFile:
    """A Python source file, read and (best-effort) AST-parsed."""

    path: Path
    rel: str
    text: str
    lines: int
    tree: Optional[ast.Module] = None
    parse_error: Optional[str] = None
    parse_error_line: Optional[int] = None
    decode_error: bool = False


@dataclass
class TextFile:
    """A non-Python text file (config, YAML, etc.) we may scan as raw text."""

    path: Path
    rel: str
    text: str


@dataclass
class AnalysisContext:
    """Everything the analyzers need, gathered once up front."""

    root: Path
    source_files: List[SourceFile] = field(default_factory=list)
    text_files: List[TextFile] = field(default_factory=list)
    # Relative directory names (top-level) that exist under root, e.g. "src".
    present_dirs: List[str] = field(default_factory=list)
    config: Dict[str, object] = field(default_factory=dict)

    def source_layer_files(self) -> List[SourceFile]:
        """Python files that live under ``src/`` (the pipeline itself)."""
        return [f for f in self.source_files if f.rel.startswith("src/")]


def find_repo_root(start: Optional[Path] = None) -> Path:
    """Walk up from ``start`` (default CWD) to the first directory that looks
    like a repo root. Falls back to the tool package's grandparent."""
    start = (start or Path.cwd()).resolve()
    candidates = [start, *start.parents]
    # Marker precedence dominates proximity: try each marker (most specific
    # first) across the whole ancestry before falling back to the next one.
    for marker in ROOT_MARKERS:
        for candidate in candidates:
            if (candidate / marker).exists():
                return candidate
    # Fallback: tools/repo_health/ -> tools/ -> <repo root>
    return _TOOL_DIR.parent.parent


def _is_excluded(path: Path, root: Path) -> bool:
    try:
        rel_parts = path.relative_to(root).parts
    except ValueError:
        return True
    if any(part in EXCLUDE_DIR_NAMES for part in rel_parts):
        return True
    # Never analyze the tool package itself.
    try:
        path.resolve().relative_to(_TOOL_DIR)
        return True
    except ValueError:
        return False


def _read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return None


def build_context(root: Path) -> AnalysisContext:
    """Read and parse the repository under ``root`` into an AnalysisContext.

    This is resilient: unreadable or unparsable files are recorded rather than
    raising, so a single malformed file never aborts the whole report.
    """
    root = root.resolve()
    ctx = AnalysisContext(root=root)

    for child in sorted(root.iterdir()):
        if child.is_dir() and child.name not in EXCLUDE_DIR_NAMES:
            ctx.present_dirs.append(child.name)

    for path in sorted(root.rglob("*")):
        if not path.is_file() or _is_excluded(path, root):
            continue
        rel = path.relative_to(root).as_posix()
        suffix = path.suffix.lower()

        if suffix == ".py":
            text = _read_text(path)
            if text is None:
                ctx.source_files.append(
                    SourceFile(path=path, rel=rel, text="", lines=0, decode_error=True)
                )
                continue
            sf = SourceFile(
                path=path, rel=rel, text=text, lines=text.count("\n") + 1
            )
            try:
                sf.tree = ast.parse(text, filename=rel)
            except SyntaxError as exc:  # noqa: BLE001 - recorded as a finding
                sf.parse_error = exc.msg
                sf.parse_error_line = exc.lineno
            ctx.source_files.append(sf)
        elif suffix in (".yml", ".yaml", ".toml", ".cfg", ".ini", ".txt", ".md"):
            text = _read_text(path)
            if text is not None:
                ctx.text_files.append(TextFile(path=path, rel=rel, text=text))

    return ctx
