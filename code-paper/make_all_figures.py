#!/usr/bin/env python3
"""Regenerate all 12 submission figures from the frozen evidence.

Single-column figures are 3.33 in wide and full-width figures are 7.0 in.
Typography, spacing, and rendering are shared across panels; plotted values
and the information carried by each figure are unchanged.
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import FancyArrow, Patch, Rectangle
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
FIG = ROOT / "paper" / "figures"
FIG.mkdir(parents=True, exist_ok=True)
EV = DATA / "evidence" / "rq2-p2"
RD9 = DATA / "results_d9"
P3 = DATA / "evidence" / "rq2-p3" / "outputs"
FIGDATA = ROOT / "code-paper" / "figdata.json"

plt.rcParams.update({
    "font.family": "serif",
    "font.serif": ["Times New Roman", "Nimbus Roman", "DejaVu Serif"],
    "mathtext.fontset": "stix",
    "font.size": 8.5,
    "axes.titlesize": 8.8,
    "axes.labelsize": 8.5,
    "axes.titleweight": "bold",
    "xtick.labelsize": 7.5,
    "ytick.labelsize": 7.5,
    "legend.fontsize": 7.3,
    "axes.linewidth": 0.6,
    "lines.linewidth": 1.3,
    "lines.markersize": 4.0,
    "xtick.major.width": 0.55,
    "ytick.major.width": 0.55,
    "xtick.major.size": 3.0,
    "ytick.major.size": 3.0,
    "axes.edgecolor": "#333333",
    "axes.labelcolor": "#222222",
    "xtick.color": "#333333",
    "ytick.color": "#333333",
    "text.color": "#222222",
    "legend.frameon": False,
    "savefig.bbox": None,
    "savefig.pad_inches": 0.025,
    "savefig.facecolor": "white",
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
})
C = {"blue": "#0072B2", "orange": "#E69F00", "green": "#009E73",
     "red": "#D55E00", "purple": "#CC79A7", "grey": "#8C8C8C"}
GRID = "#D9D9D9"
W1, W2 = 3.337, 7.005

D = json.loads(FIGDATA.read_text())
BUDGETS = [0, 265_956, 9_994_269, 49_971_343, 99_942_686]
TOTAL = 99_942_686
SHAPES = ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")


def finish_axes(ax, grid_axis="y"):
    """Apply the common low-ink academic axis treatment."""
    ax.set_axisbelow(True)
    if grid_axis:
        ax.grid(axis=grid_axis, color=GRID, linewidth=0.45, alpha=0.7)
    ax.spines[["top", "right"]].set_visible(False)


def save_figure(fig, filename, apply_layout=True):
    if apply_layout:
        fig.tight_layout(pad=0.28)
    fig.savefig(FIG / filename)
    plt.close(fig)


# ================================================================ jaffle_dag
def fig_dag():
    fig, ax = plt.subplots(figsize=(W1, 1.757))
    ax.axis("off")
    # lanes (y) and columns (x); mart-to-mart hops are right-angle elbows so
    # every arrowhead is horizontal.
    P = {"raw_supplies": (0, 4), "stg_supplies": (1.55, 4), "supplies": (5.85, 4),
         "raw_customers": (0, 3), "stg_customers": (1.55, 3), "customers": (5.85, 3),
         "raw_orders": (0, 2), "stg_orders": (1.55, 2), "orders": (4.5, 2),
         "raw_items": (0, 1), "stg_order_items": (1.55, 1), "order_items": (3.25, 1),
         "raw_products": (0, 0), "stg_products": (1.55, 0), "products": (3.25, 0)}
    straight = [("raw_supplies", "stg_supplies"), ("stg_supplies", "supplies"),
                ("raw_customers", "stg_customers"), ("raw_orders", "stg_orders"),
                ("stg_orders", "orders"), ("raw_items", "stg_order_items"),
                ("stg_order_items", "order_items"), ("raw_products", "stg_products"),
                ("stg_products", "products"), ("stg_customers", "customers")]
    elbows = [("stg_products", "order_items", 2.62),
              ("stg_supplies", "order_items", 2.42),
              ("order_items", "orders", 3.98),
              ("orders", "customers", 5.22)]
    HW, HH = 0.62, 0.27
    AC = "#666666"

    def harrow(x0, y0, x1):
        ax.add_patch(FancyArrow(x0, y0, x1 - x0, 0, width=0.001,
                                head_width=0.11, head_length=0.10,
                                length_includes_head=True, color=AC, lw=0.9))

    for a, b in straight:
        (xa, ya), (xb, yb) = P[a], P[b]
        harrow(xa + HW, ya, xb - HW)
    for a, b, xr in elbows:
        (xa, ya), (xb, yb) = P[a], P[b]
        ax.plot([xa + HW, xr], [ya, ya], color=AC, lw=0.9)
        ax.plot([xr, xr], [ya, yb], color=AC, lw=0.9)
        harrow(xr, yb, xb - HW)

    actions = {"stg_products": "82.9 ms", "products": "85.1 ms",
               "order_items": "52.4 s", "orders": "47.3 s",
               "customers": "97.9 ms"}
    sinks = {"products", "customers", "supplies"}
    inject = {"raw_products", "stg_products", "raw_orders", "raw_items"}
    for n, (x, y) in P.items():
        if n in actions:
            fc, ec = "#dbeafe", C["blue"]
        elif n.startswith("raw"):
            fc, ec = "#f2f2f2", "#888888"
        else:
            fc, ec = "#ffffff", "#888888"
        lw = 1.8 if n in sinks else 0.7
        ax.add_patch(Rectangle((x - HW, y - HH), 2 * HW, 2 * HH,
                               facecolor=fc, edgecolor=ec, linewidth=lw,
                               zorder=3))
        short = {"raw_supplies": "supplies", "raw_customers": "customers",
                 "raw_orders": "orders", "raw_items": "items",
                 "raw_products": "products", "stg_supplies": "supplies",
                 "stg_customers": "customers", "stg_orders": "orders",
                 "stg_order_items": "order\nitems",
                 "stg_products": "products",
                 "order_items": "order\nitems"}.get(n, n)
        ax.text(x, y, short, ha="center", va="center",
                fontsize=7.0 if "\n" in short else 7.4, zorder=4)
        if n in actions:
            dx = 0.04 if n != "stg_products" else 0.18
            ax.text(x + dx, y - HH - 0.10, actions[n], ha="center", va="top",
                    fontsize=6.8, color=C["blue"], style="italic")
        if n in inject:
            sx, sy = (x, y + HH + 0.16) if n == "stg_products" else (x - HW - 0.18, y)
            ax.scatter([sx], [sy], s=46, marker="*", color=C["red"],
                       zorder=5, clip_on=False)
    for x, head in ((0, "raw"), (1.55, "staging"), (4.5, "marts")):
        ax.text(x, 4.62, head, ha="center", fontsize=7.5, color="#333333",
                style="italic")
    ax.text(-0.62, 5.42, r"$\bigstar$ injection locus     "
            "shaded = candidate action node (measured policy cost)",
            fontsize=7.1, color="#333333")
    ax.text(-0.62, 5.10, "thick border = scored sink "
            "(the locations and date-spine sinks are omitted)",
            fontsize=7.1, color="#333333")
    ax.set_xlim(-0.9, 6.55)
    ax.set_ylim(-0.85, 5.6)
    save_figure(fig, "jaffle_dag.pdf")


# ================================================================ p0_collapse
def fig_p0():
    p0 = D["p0"]
    fig, ax = plt.subplots(figsize=(W1, 2.132))
    coll = [m for m, v in p0.items()
            if v["aurd"] and abs(v["aurd"] - 0.5009945650858875) < 1e-9]
    cv = p0["lineageguard_v4"]["curve"]
    ax.plot([c[0] for c in cv], [c[1] for c in cv], color=C["blue"], lw=1.8,
            label=f"{len(coll)} methods (identical curves)")
    wc = p0["weighted_min_cut"]["curve"]
    ax.plot([c[0] for c in wc], [c[1] for c in wc], color=C["red"], lw=1.2,
            ls="--", label="weighted min-cut")
    nv = p0["no_validation"]["curve"]
    ax.plot([c[0] for c in nv], [c[1] for c in nv], color=C["grey"], lw=1.0,
            ls=":", label="no validation")
    ax.annotate("NRD = 0.5008 for all 12 methods\nat every non-zero budget",
                xy=(0.47, 0.56), fontsize=7.8, color=C["blue"], ha="center")
    ax.set_xlabel("normalized budget")
    ax.set_ylabel("macro NRD")
    ax.set_ylim(0.45, 1.06)
    ax.legend(loc="center right", fontsize=7.2)
    finish_axes(ax)
    save_figure(fig, "p0_collapse.pdf")


# ================================================================ d8_heatmap
def fig_d8():
    cells = D["d8"]["cells"]
    order = ["numeric-source-low", "numeric-source-high",
             "numeric-intermediate-low", "numeric-intermediate-high",
             "duplicate-source-low", "duplicate-source-high",
             "duplicate-intermediate-low", "duplicate-intermediate-high"]
    col_lab = ["N/S/lo", "N/S/hi", "N/I/lo", "N/I/hi",
               "D/S/lo", "D/S/hi", "D/I/lo", "D/I/hi"]
    rows = [("no_validation", "no validation"),
            ("singleton.source-ecom-raw-products", "raw_products (src)"),
            ("singleton.model-stg-products", "stg_products"),
            ("singleton.model-products", "products"),
            ("singleton.model-order-items", "order_items"),
            ("singleton.model-orders", "orders"),
            ("singleton.model-customers", "customers"),
            ("all_feasible", "all feasible")]
    M = np.full((len(rows), 8), np.nan)
    bycell = {c["cell"]: c["rows"] for c in cells}
    for j, cn in enumerate(order):
        rr = bycell[cn]
        for i, (k, _) in enumerate(rows):
            if k in rr and rr[k] is not None:
                M[i, j] = rr[k]
            elif k == "all_feasible":
                for kk, vv in rr.items():
                    if kk.startswith("all"):
                        M[i, j] = vv
    fig, ax = plt.subplots(figsize=(W1, 1.943))
    norm = mcolors.TwoSlopeNorm(vmin=0.4, vcenter=1.0, vmax=23)
    cmap = plt.get_cmap("RdBu_r").copy()
    cmap.set_bad("#dddddd")
    edges = np.arange(M.shape[0] + 1) - 0.5
    im = ax.pcolormesh(edges, edges, M, cmap=cmap, norm=norm,
                       shading="flat", rasterized=False)
    ax.set_xlim(-0.5, M.shape[1] - 0.5)
    ax.set_ylim(M.shape[0] - 0.5, -0.5)
    for i in range(M.shape[0]):
        for j in range(M.shape[1]):
            if not np.isnan(M[i, j]):
                v = M[i, j]
                ax.text(j, i, f"{v:.2f}" if v < 10 else f"{v:.1f}",
                        ha="center", va="center", fontsize=7.0,
                        color="white" if (v > 6 or v < 0.55) else "black")
            else:
                ax.text(j, i, "--", ha="center", va="center", fontsize=6.8,
                        color="#888888")
    ax.set_xticks(range(8))
    ax.set_xticklabels(col_lab, fontsize=7.0, rotation=40, ha="right")
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([r[1] for r in rows], fontsize=7.4)
    ax.tick_params(length=0)
    cb = fig.colorbar(im, ax=ax, fraction=0.045, pad=0.02)
    cb.set_label("actual NRD", fontsize=7.8)
    cb.set_ticks([1, 5, 10, 15, 20])
    cb.ax.tick_params(labelsize=7.0, width=0.5, length=2.5)
    cb.outline.set_linewidth(0.5)
    save_figure(fig, "d8_heatmap.pdf")


# ================================================================ headroom
def fig_headroom():
    d9 = json.loads((RD9 / "d9-mve-summary.json").read_text())
    policies = {p["policy_id"]: p for p in d9["policy_summaries"]}
    fig, ax = plt.subplots(figsize=(W1, 1.428))
    names = ["no validation", "static optimum\n(measured ceiling)",
             "minimal conditional policy\n(measured, D9)",
             "+ dedup disposition\n(measured, D9)"]
    vals = [
        1.0,
        policies["pol-01-static-quarantine"]["macro_nrd"]["value"],
        policies["pol-05-cond-v1-minimal"]["macro_nrd"]["value"],
        policies["pol-06-cond-v2-dedup"]["macro_nrd"]["value"],
    ]
    cols = [C["grey"], C["blue"], C["green"], C["green"]]
    bars = ax.barh(range(4), vals, color=cols, edgecolor="#444444",
                   linewidth=0.4, height=0.62)
    bars[2].set_hatch("///")
    bars[3].set_hatch("///")
    for i, v in enumerate(vals):
        ax.text(v + 0.015, i, f"{v:.4f}", va="center", fontsize=7.7)
    ax.set_yticks(range(4))
    ax.set_yticklabels(names, fontsize=7.5)
    ax.invert_yaxis()
    ax.set_xlabel("macro NRD over the eight development cells")
    ax.set_xlim(0, 1.16)
    finish_axes(ax, "x")
    save_figure(fig, "headroom.pdf")


# ================================================================ pareto (P1)
def fig_pareto():
    p1 = D["p1"]["campaigns"]
    costs = D["costs"]
    static_ceiling = 31 / 38
    all5 = float(np.mean([c["all5_nrd"] for c in p1]))
    fig, ax = plt.subplots(figsize=(W1, 2.069))
    pts = [
        ("none", 1, 1.0, (5, 6), "left"),
        ("{products}", costs["products"], static_ceiling, (-16, 12), "center"),
        ("{stg_products}", costs["stg_products"], static_ceiling,
         (7, -15), "left"),
        ("{products,\ncustomers}", costs["products"] + costs["customers"],
         static_ceiling, (9, 12), "left"),
        ("all five", sum(costs.values()), all5, (-5, 9), "right"),
    ]
    for name, cost, nrd, offset, align in pts:
        ax.scatter(max(cost, 1), nrd, s=28, color=C["blue"],
                   edgecolor="white", linewidth=0.35, zorder=3)
        ax.annotate(name, (max(cost, 1), nrd), textcoords="offset points",
                    xytext=offset, fontsize=7.3, ha=align, va="center")
    ax.axhline(static_ceiling, color=C["grey"], lw=0.65, ls=":")
    ax.text(0.03, 0.61, "three static plans: equal damage,\n"
            "4.8x spend difference, invisible to AURD",
            fontsize=7.2, color=C["blue"], transform=ax.transAxes)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("actual plan spend (µs, log)")
    ax.set_ylabel("macro NRD (log)")
    ax.set_ylim(0.25, 30)
    finish_axes(ax, "both")
    save_figure(fig, "pareto.pdf")


# ================================================================ d9_ladder
def fig_d9():
    d9 = json.loads((RD9 / "d9-mve-summary.json").read_text())
    policies = {p["policy_id"]: p for p in d9["policy_summaries"]}
    spec = [
        ("pol-00-no-validation", "none", "anchor"),
        ("pol-01-static-quarantine", "quar.\nD1", "static"),
        ("pol-02-static-dedup", "dedup\nD2", "static"),
        ("pol-03-static-nullout", "null-out\nD3", "static"),
        ("pol-05-cond-v1-minimal", "v1 q./\nno-op", "conditional"),
        ("pol-06-cond-v2-dedup", "v2 q./\ndedup", "conditional"),
        ("pol-07-cond-v2-nullout", "v2+ n.o./\ndedup", "conditional"),
    ]
    colmap = {
        "anchor": C["grey"],
        "static": C["blue"],
        "conditional": C["green"],
    }
    fig, ax = plt.subplots(figsize=(W1, 2.703))
    xs = np.arange(len(spec))
    for i, (pid, label, policy_class) in enumerate(spec):
        value = policies[pid]["macro_nrd"]["value"]
        bar = ax.bar(i, value, width=0.72, color=colmap[policy_class],
                     edgecolor="#444444", linewidth=0.4)
        if policies[pid].get("clears_sesoi"):
            bar[0].set_hatch("///")
        ax.text(i, value + 0.045, f"{value:.3f}", ha="center",
                va="bottom", fontsize=6.8)
    static_ceiling = policies["pol-01-static-quarantine"]["macro_nrd"]["value"]
    ax.axhline(1.0, color="#333333", lw=0.75, ls="--")
    ax.text(6.45, 1.035, "no validation", fontsize=6.9, ha="right",
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 0.4})
    ax.axhline(static_ceiling, color=C["orange"], lw=0.9, ls=":")
    ax.text(6.42, static_ceiling - 0.025, "static ceiling\n= 0.8158",
            fontsize=6.8, ha="right", va="top", color=C["orange"],
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 0.3})
    ax.set_xticks(xs)
    ax.set_xticklabels([item[1] for item in spec], fontsize=6.7)
    ax.set_ylabel("macro NRD (8 dev cells)")
    ax.set_ylim(0, 2.18)
    ax.legend(
        handles=[
            Patch(color=C["grey"], label="anchor"),
            Patch(color=C["blue"], label="static policy"),
            Patch(color=C["green"], label="conditional policy"),
            Patch(facecolor="white", edgecolor="#444444", hatch="///",
                  label="clears 10% SESOI"),
        ],
        ncol=2,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.005),
        fontsize=6.9,
        handlelength=1.1,
        columnspacing=1.0,
        handletextpad=0.45,
    )
    finish_axes(ax)
    save_figure(fig, "d9_ladder.pdf")


# ================================================================ d10
def fig_d10():
    d10 = json.loads((RD9 / "d10-summary.json").read_text())
    policies = {p["policy_id"]: p for p in d10["policy_summaries"]}
    nodes = [
        "model:stg_products",
        "model:products",
        "model:order_items",
        "model:orders",
        "model:customers",
    ]
    labels = ["stg_\nproducts", "products", "order_\nitems", "orders", "customers"]
    fig, ax = plt.subplots(figsize=(W1, 2.765))
    xs = np.arange(len(nodes))
    width = 0.36
    series = [
        ("d10-p1-quarantine", "quarantine only (D1)", C["blue"]),
        ("d10-p2-cond-quar-dedup", "conditional: quar./dedup", C["green"]),
    ]
    for series_index, (pid, label, color) in enumerate(series):
        values = [
            policies[pid]["per_placement"][node]["macro_nrd"]["value"]
            for node in nodes
        ]
        positions = xs + (series_index - 0.5) * width
        ax.bar(positions, values, width, color=color, edgecolor="#444444",
               linewidth=0.4, label=label)
        for x_pos, value in zip(positions, values):
            if value >= 0.99:
                y_offset = 0.025 if series_index == 0 else 0.095
                ax.text(x_pos, value - y_offset, f"{value:.3f}",
                        ha="center", va="top", fontsize=7.0, color="white")
            else:
                ax.text(x_pos, value + 0.025, f"{value:.3f}",
                        ha="center", va="bottom", fontsize=6.6)
    ax.axhline(1.0, color="#333333", lw=0.75, ls="--")
    ax.text(4.45, 1.015, "no validation", fontsize=6.8, ha="right",
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 0.3})
    ax.set_xticks(xs)
    ax.set_xticklabels(labels, fontsize=6.9)
    ax.set_xlabel(r"placement node (upstream $\rightarrow$ downstream)")
    ax.set_ylabel("macro NRD")
    ax.set_ylim(0, 1.22)
    ax.legend(
        ncol=2,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.0),
        fontsize=6.7,
        handlelength=1.1,
        columnspacing=0.8,
        handletextpad=0.4,
    )
    finish_axes(ax)
    save_figure(fig, "d10_position_policy.pdf")


# ================================================================ cost comp.
def fig_cost():
    catalog = json.loads((RD9 / "policy-cost-catalog.json").read_text())
    by_node_disposition = {
        (row["node_id"], row["disposition"]): row
        for row in catalog["catalog"]
        if row.get("status") == "priced"
    }
    node_ids = [
        "model:stg_products",
        "model:products",
        "model:order_items",
        "model:orders",
        "model:customers",
    ]
    rows = []
    for node_id in node_ids:
        row = by_node_disposition[(node_id, "dedup")]
        rows.append((
            node_id.split(":", 1)[1],
            row["c_deploy_us"],
            round(row["c_detect_us"]),
            round(row["c_disposition_us"]),
        ))

    fig, ax = plt.subplots(figsize=(W1, 2.113))
    ys = np.arange(len(rows))[::-1]
    for y_pos, (node, deploy, detect, disposition) in zip(ys, rows):
        ax.barh(y_pos, deploy, color="#A6A6A6", edgecolor="#444444",
                linewidth=0.3)
        ax.barh(y_pos, detect, left=deploy, color=C["orange"],
                edgecolor="#444444", linewidth=0.3)
        ax.barh(y_pos, max(disposition, 1.0), left=deploy + detect,
                color=C["purple"], edgecolor="#444444", linewidth=0.3)
        ratio = (deploy + detect + disposition) / deploy
        ratio_label = f"{ratio:.1f}x" if ratio >= 3 else f"{ratio:.2f}x"
        ax.text(deploy + detect + max(disposition, 1.0), y_pos,
                "  " + ratio_label, va="center", fontsize=8.5)
    ax.set_yticks(ys)
    ax.set_yticklabels([row[0] for row in rows], fontsize=8.5,
                       family="monospace")
    ax.set_xscale("log")
    ax.set_xlabel("cost (µs, log scale)")
    ax.set_xlim(2e3, 6e8)
    ax.legend(
        handles=[
            Patch(color="#A6A6A6", label=r"$C_{deploy}$ (frozen)"),
            Patch(color=C["orange"], label=r"$C_{detect}$ (new)"),
            Patch(color=C["purple"], label=r"$C_{disp}$ (new)"),
        ],
        ncol=2,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.005),
        fontsize=8.5,
        handlelength=1.0,
        columnspacing=0.75,
        handletextpad=0.35,
    )
    finish_axes(ax, "x")
    save_figure(fig, "cost_components.pdf")


# ================================================================ p2 figures
def plan_sig(plan):
    return json.dumps([{"node": p["node"],
                        "map": {s: p["map"].get(s, "no_op") for s in SHAPES}}
                       for p in plan], sort_keys=True, separators=(",", ":"))


def load_p2():
    plans = json.loads((EV / "freeze" / "p2-plans.json").read_text())
    summary = json.loads((EV / "outputs" / "p2-summary.json").read_text())
    rows = []
    for i in (1, 2, 3, 4):
        rows += json.loads((EV / "outputs" / f"p2-shard{i}-slim.json")
                           .read_text())["results"]
    phys = {(r["campaign_id"], r["plan_sig"]): r for r in rows
            if r["role"] == "physical_plan"}
    campaigns = sorted({r["campaign_id"] for r in rows})
    binding = {}
    for method, entries in plans["methods"].items():
        for e in entries:
            sig = plan_sig(e["plan"]) if e["plan"] else None
            for cid in campaigns:
                binding[(method, e["budget_us"], cid)] = (
                    1.0 if sig is None else phys[(cid, sig)]["nrd"])
    return plans, summary, binding, campaigns


MLAB = {"policy_planner": ("LineageGuard (policy)", "#0072B2", "-", "o"),
        "static_best": ("strongest static set", "#D55E00", "--", "s"),
        "static_quarantine_legacy": ("legacy static quarantine", "#7a7a7a",
                                     ":", "^"),
        "policy_all_feasible": ("policy at all nodes", "#009E73", "-.", "d")}


def fig_p2():
    _, summary, binding, campaigns = load_p2()
    fig, axes = plt.subplots(
        1,
        3,
        figsize=(W2, 2.525),
        gridspec_kw={"width_ratios": [1.10, 0.95, 1.32]},
    )
    handles = []

    ax = axes[0]
    budget_fraction = [budget / TOTAL for budget in BUDGETS]
    for method, (label, color, linestyle, marker) in MLAB.items():
        values = [
            np.mean([binding[(method, budget, campaign)]
                     for campaign in campaigns])
            for budget in BUDGETS
        ]
        handle, = ax.plot(
            budget_fraction,
            values,
            linestyle,
            color=color,
            marker=marker,
            markerfacecolor="white" if method == "static_best" else color,
            markeredgewidth=0.7,
            ms=4.0,
            lw=1.35,
            label=label,
        )
        handles.append(handle)
    ax.axhline(1.0, color="#444444", lw=0.65, alpha=0.7)
    ax.set_xlabel("normalized budget")
    ax.set_ylabel("macro NRD (16 campaigns)")
    ax.set_title("(a) residual vs. budget")
    finish_axes(ax)

    ax = axes[1]
    diffs = sorted(summary["primary_comparison"]["diffs"])
    campaign_index = np.arange(len(diffs))
    colors = [C["blue"] if value < 0 else "#B8B8B8" for value in diffs]
    ax.bar(campaign_index, diffs, color=colors, width=0.78)
    zero_mask = np.isclose(diffs, 0.0)
    ax.scatter(campaign_index[zero_mask], np.zeros(np.sum(zero_mask)),
               s=10, color="#8C8C8C", zorder=3, clip_on=False)
    ax.axhline(0, color="#333333", lw=0.65)
    ax.set_xlim(-0.7, len(diffs) - 0.3)
    ax.set_ylim(min(diffs) * 1.08, 0.025)
    ax.set_xticks([0, 5, 10, 15])
    ax.set_xlabel("campaign (sorted)")
    ax.set_ylabel(r"paired $\Delta$AURD")
    ax.set_title("(b) 16 paired differences")
    ax.text(
        0.97,
        0.07,
        "mean $-0.148$\nCI $[-0.266,-0.059]$\n$p=0.03125$",
        transform=ax.transAxes,
        fontsize=7.2,
        va="bottom",
        ha="right",
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.85,
              "pad": 1.0},
    )
    finish_axes(ax)

    ax = axes[2]
    families = [
        "prod-num",
        "prod-dup",
        "ord-dup",
        "ord-num",
        "ord-mixed",
        "null-prod",
        "fk-ord",
        "del-ord",
    ]
    family_labels = ["p-num", "p-dup", "o-dup", "o-num",
                     "mixed", "null", "fk", "del"]
    per_family = summary["per_family_mean_aurd"]
    width = 0.27
    for series_index, method in enumerate([
        "policy_planner",
        "static_best",
        "static_quarantine_legacy",
    ]):
        _, color, _, _ = MLAB[method]
        positions = np.arange(len(families)) + (series_index - 1) * width
        ax.bar(
            positions,
            [per_family[family][method] for family in families],
            width=width,
            color=color,
            edgecolor="white",
            linewidth=0.25,
        )
    ax.set_xticks(range(len(families)))
    ax.set_xticklabels(family_labels, fontsize=7.1, rotation=32, ha="right")
    ax.axhline(1.0, color="#444444", lw=0.65, alpha=0.7)
    ax.set_ylabel("mean AURD")
    ax.set_ylim(0, 1.18)
    ax.set_title("(c) per-family attribution")
    finish_axes(ax)

    fig.legend(
        handles=handles,
        ncol=4,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.995),
        fontsize=7.3,
        handlelength=1.8,
        columnspacing=1.2,
        handletextpad=0.5,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.91), w_pad=1.25)
    save_figure(fig, "p2_main.pdf", apply_layout=False)

def fig_rqf():
    data = json.loads(
        (EV / "outputs" / "rqf-full02" /
         "rqf-stress-measurement.json").read_text()
    )
    fig, axes = plt.subplots(1, 2, figsize=(W1, 1.855), sharey=True)
    panels = (
        (axes[0], "orders", ["+\\$20", "+\\$110", "+\\$250", "+\\$100k"]),
        (axes[1], "products", ["+\\$2", "+\\$8", "+\\$100"]),
    )
    for ax, family, labels in panels:
        rows = sorted(
            [row for row in data["results"] if row["family"] == family],
            key=lambda row: row["injection"]["operand"],
        )
        positions = np.arange(len(rows))
        nrd = [row["nrd"] for row in rows]
        detected = [row["detected"] for row in rows]
        ax.plot(positions, nrd, "-o", ms=4.2, color=C["blue"],
                markeredgecolor="white", markeredgewidth=0.45,
                lw=1.3, zorder=3)
        for x_pos, (value, did_detect) in enumerate(zip(nrd, detected)):
            vertical_offset = -12 if value > 0.7 else 9
            ax.annotate(
                "hit" if did_detect else "miss",
                (x_pos, value),
                xytext=(0, vertical_offset),
                textcoords="offset points",
                ha="center",
                fontsize=7.2,
                color=C["green"] if did_detect else C["red"],
            )
        ax.set_xticks(positions)
        ax.set_xticklabels(
            [label.replace("\\", "") for label in labels],
            fontsize=7.1,
            rotation=20 if len(rows) > 3 else 0,
        )
        ax.set_xlim(-0.5, len(rows) - 0.5)
        ax.set_ylim(0.34, 1.14)
        ax.axhline(1.0, color="#444444", lw=0.65, alpha=0.7)
        ax.set_title(f"{family} fork")
        finish_axes(ax)
    axes[0].set_ylabel("NRD under policy")
    fig.tight_layout(w_pad=0.9)
    save_figure(fig, "rqf_stress.pdf")

def fig_scaling():
    data = json.loads((DATA / "planner-scaling.json").read_text())
    rows = [row for row in data["results"] if row["n_classes"] == 8]
    fig, ax = plt.subplots(figsize=(W1, 2.089))
    series = (
        ("naive_s", "exhaustive enumeration", C["red"], "s"),
        ("bnb_s", "branch-and-bound", "#6F6F6F", "^"),
        ("eqbnb_s", "equivalence-reduced B&B", C["blue"], "o"),
    )
    for key, label, color, marker in series:
        points = [(row["n_nodes"], row[key]) for row in rows if key in row]
        ax.plot(
            [point[0] for point in points],
            [max(point[1], 1e-5) for point in points],
            "-",
            marker=marker,
            ms=4.0,
            lw=1.3,
            color=color,
            markeredgecolor="white",
            markeredgewidth=0.4,
            label=label,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel(r"candidate nodes $|V|$")
    ax.set_ylabel("planning time (s)")
    ax.set_ylim(1.5e-5, 4)
    ax.legend(loc="lower right", fontsize=7.0, handlelength=1.7,
              labelspacing=0.35)
    finish_axes(ax, "both")
    save_figure(fig, "planner_scaling.pdf")


# ================================================================ p3_dose
def fig_p3(summary_path=None, dose_path=None, out_path=None):
    summary_path = summary_path or P3 / "p3-summary.json"
    dose_path = dose_path or P3 / "p3-dose-analysis.json"
    summary = json.loads(Path(summary_path).read_text())
    dose = json.loads(Path(dose_path).read_text())
    rows = sorted(dose["rows"], key=lambda row: row["w_fresh"])
    assert len(rows) == 5
    assert dose["max_abs_residual"] <= 2e-16
    assert len(summary["primary_comparison"]["campaigns"]) == 18

    fig, ax = plt.subplots(figsize=(W1, 2.279))
    f_grid = dose["f_grid"]
    reference_x = np.linspace(0.0, 1.0, 101)
    q_one = 10 / 19
    ax.plot(
        reference_x,
        [(q_one - 1) * value * f_grid for value in reference_x],
        color=C["grey"],
        lw=0.9,
        ls="--",
        label=r"$k{=}1$ reference line",
    )
    x_values = [row["w_fresh"] for row in rows]
    predicted = [row["predicted_diff"] for row in rows]
    observed = [row["observed_diff"] for row in rows]
    ax.plot(
        x_values,
        predicted,
        color=C["orange"],
        lw=1.2,
        label="pre-registered prediction",
    )
    ax.scatter(
        x_values,
        observed,
        s=28,
        color=C["blue"],
        edgecolor="white",
        linewidth=0.45,
        zorder=3,
        label="measured (fresh)",
    )
    for row in rows:
        k_num = row["k_num"]
        vertical_offset = -11 if k_num != 9 else 8
        ax.annotate(
            str(k_num) + r"$\times$" + str(10 - k_num),
            (row["w_fresh"], row["observed_diff"]),
            textcoords="offset points",
            xytext=(0, vertical_offset),
            ha="center",
            fontsize=7.0,
            color=C["grey"],
        )
    ax.axhline(0, color="#333333", lw=0.6)
    ax.set_xlabel(r"numeric damage share $w$ (fresh anchors)")
    ax.set_ylabel(r"paired $\Delta$AURD")
    ax.set_xlim(0, 1.02)
    ax.legend(loc="lower left", fontsize=7.1, handlelength=1.7,
              labelspacing=0.35)
    ax.text(
        0.55,
        0.96,
        r"max $|$resid$|$ $= 1.1\times10^{-16}$",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=7.5,
    )
    finish_axes(ax)
    if out_path is None:
        save_figure(fig, "p3_dose.pdf")
    else:
        fig.savefig(Path(out_path))
        plt.close(fig)

if __name__ == "__main__":
    builders = [
        ("jaffle_dag", fig_dag),
        ("p0_collapse", fig_p0),
        ("d8_heatmap", fig_d8),
        ("headroom", fig_headroom),
        ("pareto", fig_pareto),
        ("d9_ladder", fig_d9),
        ("d10_position_policy", fig_d10),
        ("cost_components", fig_cost),
        ("p2_main", fig_p2),
        ("rqf_stress", fig_rqf),
        ("planner_scaling", fig_scaling),
        ("p3_dose", fig_p3),
    ]
    for name, builder in builders:
        builder()
        print(name)
