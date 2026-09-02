"""Analyzer base class."""

from __future__ import annotations

import abc

from tools.repo_health.core import CategoryReport
from tools.repo_health.discovery import AnalysisContext


class Analyzer(abc.ABC):
    """An analyzer inspects the :class:`AnalysisContext` and returns one
    :class:`CategoryReport`. Implementations must be resilient: never raise on
    malformed input, surface it as a finding instead."""

    key: str = ""
    title: str = ""

    @abc.abstractmethod
    def analyze(self, ctx: AnalysisContext) -> CategoryReport:  # pragma: no cover
        raise NotImplementedError

    def _report(self) -> CategoryReport:
        return CategoryReport(key=self.key, title=self.title)
