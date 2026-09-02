"""Entry point so the tool runs as ``python -m tools.repo_health``."""

from tools.repo_health.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
