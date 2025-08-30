"""
Autodiscovers report modules inside this package.
We keep actual report modules under `src/reports/reports/` so that
markdown docs can live alongside code.
"""

from .reports import *  # noqa: F401,F403 - re-export discovered reports

