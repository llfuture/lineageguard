#!/usr/bin/env python3
"""Compatibility entry point for the single-column P3 dose-response figure.

Usage: make_p3_figure.py p3-summary.json p3-dose-analysis.json [out.pdf]
"""
from pathlib import Path
import sys

from make_all_figures import fig_p3


def main():
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    output = Path(sys.argv[3]) if len(sys.argv) > 3 else Path("p3_dose.pdf")
    fig_p3(Path(sys.argv[1]), Path(sys.argv[2]), output)
    print("wrote", output)


if __name__ == "__main__":
    main()
