"""Part 4 - four static charts from the CSVs produced by the Mongo / Spark / integration scripts.

Usage: python viz/charts.py [--data outputs] [--out outputs]
Each chart answers one business question; the one-sentence takeaway is printed and written into the PNG title area.
"""
import argparse, csv, os
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ap = argparse.ArgumentParser()
ap.add_argument("--data", default="outputs")
ap.add_argument("--out", default="outputs")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)

# validated categorical order (blue, orange, aqua, yellow, magenta) + neutral grey for "Other"
C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
OTHER, INK, MUTED, GRID = "#9a9a96", "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED,
                     "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
                     "figure.facecolor": "white", "axes.facecolor": "white", "font.family": "DejaVu Sans"})

def rows(name):
    with open(os.path.join(a.data, name), encoding="utf-8") as f:
        return list(csv.DictReader(f))

def finish(fig, ax_title, takeaway, fname):
    import textwrap
    fig.text(0.01, 0.985, ax_title, ha="left", va="top", fontsize=13, fontweight="bold", color=INK)
    fig.text(0.01, 0.93, "\n".join(textwrap.wrap(takeaway, 105)), ha="left", va="top", fontsize=9.5, color=MUTED)
    fig.tight_layout(rect=(0, 0, 1, 0.84))
    p = os.path.join(a.out, fname)
    fig.savefig(p, dpi=200)
    plt.close(fig)
    print(f"{fname}: {takeaway}")

# ---------- 1) Revenue by category over time (stacked: top-5 categories + Other) ----------
rev = rows("mongo_revenue_by_category_month.csv")
tot_cat, by_month = defaultdict(float), defaultdict(lambda: defaultdict(float))
for r in rev:
    v = float(r["revenue"]); tot_cat[r["category"]] += v; by_month[r["month"]][r["category"]] += v
top = [c for c, _ in sorted(tot_cat.items(), key=lambda kv: -kv[1])[:5]]
months = sorted(by_month)
fig, ax = plt.subplots(figsize=(9, 4.8))
bottom = [0.0] * len(months)
for i, c in enumerate(top + ["Other"]):
    vals = [by_month[m][c] if c != "Other" else sum(v for k, v in by_month[m].items() if k not in top) for m in months]
    ax.bar(months, vals, bottom=bottom, color=C[i] if c != "Other" else OTHER, label=c, edgecolor="white", linewidth=1.2, width=0.7)
    bottom = [b + v for b, v in zip(bottom, vals)]
ax.set_ylabel("Revenue (item subtotals, before order discount)")
ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k"))
ax.legend(frameon=False, fontsize=8, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.12))
ax.grid(axis="x", visible=False)
full = [m for m in months[1:-1]]
peak = max(full, key=lambda m: sum(by_month[m].values())) if full else months[0]
share = 100 * sum(tot_cat[c] for c in top) / sum(tot_cat.values())
finish(fig, "Monthly revenue by category",
       f"Revenue is spread thinly: the top 5 of {len(tot_cat)} categories make up only {share:.0f}% of it; {peak} is the strongest full month "
       f"(first and last months are partial).", "chart1_revenue_by_category.png")

# ---------- 2) Funnel (overall) + conversion by customer segment ----------
fun = rows("funnel.csv")
ov = next(r for r in fun if r["dimension"] == "overall")
stages = [("Sessions", "sessions"), ("Viewed a product", "viewed"), ("Added to cart", "carted"),
          ("Reached checkout", "checkout"), ("Purchased", "purchased")]
vals = [int(ov[k]) for _, k in stages]
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.4), gridspec_kw={"width_ratios": [1.25, 1]})
ys = list(range(len(stages)))[::-1]
ax1.barh(ys, vals, color=C[0], height=0.6)
for y, v, (lbl, _) in zip(ys, vals, stages):
    ax1.text(v + max(vals) * 0.01, y, f"{v:,}  ({100 * v / vals[0]:.0f}%)", va="center", fontsize=9, color=INK)
ax1.set_yticks(ys); ax1.set_yticklabels([s for s, _ in stages]); ax1.set_xlim(0, max(vals) * 1.35)
ax1.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
ax1.tick_params(axis="x", labelsize=8); ax1.grid(axis="y", visible=False)
ax1.set_title("Sessions reaching each stage", fontsize=10, color=MUTED, loc="left")
seg = [r for r in fun if r["dimension"] == "customer_segment"]
order = ["0 orders", "1 order", "2-3 orders", "4-6 orders", "7+ orders"]
seg = sorted(seg, key=lambda r: order.index(r["value"]))
conv = [100 * float(r["overall_conversion"]) for r in seg]
ax2.bar([r["value"] for r in seg], conv, color=C[0], width=0.6)
for i, v in enumerate(conv): ax2.text(i, v + 0.5, f"{v:.1f}%", ha="center", fontsize=9, color=INK)
ax2.set_ylabel("Session to purchase conversion (%)"); ax2.set_ylim(0, max(conv) * 1.2); ax2.grid(axis="x", visible=False)
ax2.tick_params(axis="x", labelsize=8)
ax2.set_title("Conversion by customer order frequency", fontsize=10, color=MUTED, loc="left")
drops = [(stages[i][0], stages[i + 1][0], 1 - vals[i + 1] / vals[i]) for i in range(1, 4)]
big = max(drops, key=lambda d: d[2])
def spread(dim):
    v = [100 * float(r["overall_conversion"]) for r in fun if r["dimension"] == dim]
    return max(v) - min(v)
sp = max(spread("device"), spread("referrer"))
seg_note = (f"device and referrer differ by under {sp + 0.5:.0f} percentage point(s)" if sp < 1.5
            else f"device/referrer differ by up to {sp:.1f} points")
finish(fig, "Conversion funnel (HBase sessions + MongoDB purchases)",
       f"The biggest drop is {big[0].lower()} to {big[1].lower()} ({100 * big[2]:.0f}% lost); overall conversion is "
       f"{100 * vals[-1] / vals[0]:.1f}% and varies from {min(conv):.1f}% to {max(conv):.1f}% across customer order-frequency bands, while {seg_note}.",
       "chart2_funnel.png")

# ---------- 3) Most viewed vs top selling ----------
vu = rows("mongo_views_vs_units.csv")
x = [float(r["views"]) for r in vu]; y = [float(r["units"]) for r in vu]
fig, ax = plt.subplots(figsize=(8, 5))
ax.scatter(x, y, s=14, color=C[0], alpha=0.55, edgecolors="none")
for r in sorted(vu, key=lambda r: -float(r["units"]))[:5]:
    ax.annotate(r["product_id"], (float(r["views"]), float(r["units"])), textcoords="offset points", xytext=(5, 4), fontsize=8, color=INK)
ax.set_xlabel("Product-page views (loaded sessions)"); ax.set_ylabel("Units sold")
n = len(x); mx, my = sum(x) / n, sum(y) / n
cov = sum((p - mx) * (q - my) for p, q in zip(x, y))
r_ = cov / ((sum((p - mx) ** 2 for p in x) * sum((q - my) ** 2 for q in y)) ** 0.5 or 1)
finish(fig, "Most viewed vs top selling products",
       f"Views are a {'strong' if abs(r_) > 0.7 else 'moderate' if abs(r_) > 0.4 else 'weak'} predictor of sales (correlation {r_:.2f}): "
       f"{n} products plotted; the five best sellers are labelled.", "chart3_views_vs_units.png")

# ---------- 4) User segmentation: share of users vs share of revenue ----------
sg = rows("mongo_user_segments.csv")
import re
sg = sorted(sg, key=lambda r: int(re.match(r"\d+", r["segment"]).group()))
tu = sum(int(r["users"]) for r in sg)
us = [100 * int(r["users"]) / tu for r in sg]; rv = [float(r["share_of_revenue_pct"]) for r in sg]
fig, ax = plt.subplots(figsize=(8.5, 4.8))
w = 0.38; idx = range(len(sg))
ax.bar([i - w / 2 for i in idx], us, w, color=C[0], label="Share of customers", edgecolor="white", linewidth=1.2)
ax.bar([i + w / 2 for i in idx], rv, w, color=C[1], label="Share of revenue", edgecolor="white", linewidth=1.2)
for i, (p, q) in enumerate(zip(us, rv)):
    ax.text(i - w / 2, p + 0.6, f"{p:.0f}%", ha="center", fontsize=8, color=INK)
    ax.text(i + w / 2, q + 0.6, f"{q:.0f}%", ha="center", fontsize=8, color=INK)
ax.set_xticks(list(idx)); ax.set_xticklabels([f"{r['segment']}\n(avg {float(r['avg_spend']):,.0f})" for r in sg], fontsize=8)
ax.set_ylabel("%"); ax.legend(frameon=False); ax.grid(axis="x", visible=False)
hv = max(range(len(sg)), key=lambda i: rv[i])
lead = max((i for i in range(len(sg)) if us[i] >= 5), key=lambda i: rv[i] / us[i])
finish(fig, "Customer segments by purchase frequency",
       f"The '{sg[hv]['segment']}' band is {us[hv]:.0f}% of customers and {rv[hv]:.0f}% of revenue; the '{sg[lead]['segment']}' band earns "
       f"{rv[lead] / us[lead]:.1f}x its customer share (average spend per customer under each label).", "chart4_segmentation.png")
