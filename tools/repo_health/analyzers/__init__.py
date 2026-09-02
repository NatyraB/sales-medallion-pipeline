"""Analyzer registry.

``ANALYZERS`` is the ordered list of analyzer instances the CLI runs. The order
here is the order categories appear in the report.
"""

from __future__ import annotations

from typing import List

from tools.repo_health.analyzers.base import Analyzer
from tools.repo_health.analyzers.code_quality import CodeQualityAnalyzer
from tools.repo_health.analyzers.dependencies import DependencyAnalyzer
from tools.repo_health.analyzers.hotspots import HotspotAnalyzer
from tools.repo_health.analyzers.security import SecurityAnalyzer
from tools.repo_health.analyzers.structure import StructureAnalyzer
from tools.repo_health.analyzers.testing import TestingAnalyzer


def default_analyzers() -> List[Analyzer]:
    return [
        CodeQualityAnalyzer(),
        StructureAnalyzer(),
        TestingAnalyzer(),
        DependencyAnalyzer(),
        SecurityAnalyzer(),
        HotspotAnalyzer(),
    ]


__all__ = ["Analyzer", "default_analyzers"]
