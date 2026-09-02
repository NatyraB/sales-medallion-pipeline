"""Medallion structure & naming analyzer.

Checks that the bronze/silver/gold layers exist and that files within a layer
follow a consistent, unambiguous ``NN_name.py`` numbering scheme. Duplicate
numeric prefixes within a layer are the headline finding for this repo.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Dict, List

from tools.repo_health.analyzers.base import Analyzer
from tools.repo_health.core import CategoryReport, Finding, Severity
from tools.repo_health.discovery import AnalysisContext

MEDALLION_LAYERS = ("bronze", "silver", "gold")
PREFIX_RE = re.compile(r"^(\d+)_(.+)\.py$")
SNAKE_RE = re.compile(r"^[a-z0-9_]+$")


class StructureAnalyzer(Analyzer):
    key = "structure"
    title = "Medallion Structure & Naming"

    def analyze(self, ctx: AnalysisContext) -> CategoryReport:
        report = self._report()

        # Group src files by their top-level layer directory.
        by_layer: Dict[str, List[str]] = defaultdict(list)
        for sf in ctx.source_layer_files():
            parts = sf.rel.split("/")
            if len(parts) >= 3 and parts[0] == "src":
                by_layer[parts[1]].append("/".join(parts[2:]))

        present = [layer for layer in MEDALLION_LAYERS if layer in by_layer]
        missing = [layer for layer in MEDALLION_LAYERS if layer not in by_layer]

        for layer in missing:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.MEDIUM,
                    title=f"Missing `{layer}` layer",
                    detail=f"No `src/{layer}/` directory found.",
                    recommendation=f"Add a `{layer}` layer or document why it is absent.",
                )
            )

        prefix_dupes = 0
        bad_names: List[str] = []

        for layer in present:
            files = sorted(by_layer[layer])
            prefixes: Dict[str, List[str]] = defaultdict(list)
            for name in files:
                match = PREFIX_RE.match(name)
                if match:
                    prefixes[match.group(1)].append(name)
                    stem = match.group(2)
                    if not SNAKE_RE.match(stem):
                        bad_names.append(f"src/{layer}/{name}")
                elif name.endswith(".py"):
                    bad_names.append(f"src/{layer}/{name}")

            for prefix, names in sorted(prefixes.items()):
                if len(names) > 1:
                    prefix_dupes += 1
                    locs = ", ".join(f"src/{layer}/{n}" for n in sorted(names))
                    report.add(
                        Finding(
                            category=self.key,
                            severity=Severity.MEDIUM,
                            title=f"Duplicate `{prefix}_` prefix in `{layer}` layer",
                            detail=(
                                f"{len(names)} files share the numeric prefix `{prefix}_`, "
                                f"making execution order ambiguous: {locs}."
                            ),
                            recommendation=(
                                "Renumber files so each layer has a unique, sequential prefix "
                                "reflecting run order."
                            ),
                        )
                    )

        if bad_names:
            report.add(
                Finding(
                    category=self.key,
                    severity=Severity.LOW,
                    title=f"Non-standard file names ({len(bad_names)})",
                    detail=(
                        "Files not matching the `NN_snake_case.py` convention: "
                        + ", ".join(sorted(bad_names))
                    ),
                    recommendation="Rename to `NN_snake_case.py` for a consistent, sortable layout.",
                )
            )

        # Detect that two distinct pipelines coexist under the same layers
        # (accounts vs. orders/customers/products) — worth surfacing, not a defect.
        gold_files = by_layer.get("gold", [])
        family_hint = any("gold_" in n for n in gold_files) and any(
            "gold_" not in n for n in gold_files
        )
        if family_hint:
            report.notes.append(
                "Two naming families coexist in `gold` (`NN_gold_*` vs `NN_*`), suggesting two "
                "pipelines share the layer dirs."
            )

        report.metrics = {
            "layers_present": present,
            "layers_missing": missing,
            "files_per_layer": {layer: len(by_layer.get(layer, [])) for layer in MEDALLION_LAYERS},
            "duplicate_prefix_groups": prefix_dupes,
            "nonstandard_names": len(bad_names),
        }
        if present == list(MEDALLION_LAYERS):
            report.notes.append("All three medallion layers (bronze/silver/gold) are present.")
        return report
