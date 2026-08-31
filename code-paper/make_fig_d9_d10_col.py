#!/usr/bin/env python3
"""Compatibility entry point for the single-column D9 and D10 figures.

The canonical implementations live in make_all_figures.py so this legacy
entry point cannot drift in typography, layout, or data paths.
"""
from make_all_figures import fig_d10, fig_d9


if __name__ == "__main__":
    fig_d9()
    print("d9_ladder")
    fig_d10()
    print("d10_position_policy")
