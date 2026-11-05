"""Builds report/Technical_Report.pdf from the charts in outputs/. Usage: python report/build_report.py [outputs_dir]
Edit AUTHOR / STUDENT_ID below first."""
import os, sys
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, KeepTogether, PageBreak
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon

AUTHOR = "Your Name"          # <-- edit
STUDENT_ID = "Student ID"     # <-- edit
REPO = "https://github.com/KWIZERARENE/auca-bigdata-ecommerce"
OUT = sys.argv[1] if len(sys.argv) > 1 else "outputs"
PDF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Technical_Report.pdf")

ss = getSampleStyleSheet()
B = ParagraphStyle("B", parent=ss["BodyText"], fontName="Helvetica", fontSize=9.4, leading=12.6, spaceAfter=5)
H1 = ParagraphStyle("H1", parent=ss["Heading1"], fontName="Helvetica-Bold", fontSize=13.5, spaceBefore=9, spaceAfter=4, textColor=colors.HexColor("#1f3a5f"))
H2 = ParagraphStyle("H2", parent=ss["Heading2"], fontName="Helvetica-Bold", fontSize=10.5, spaceBefore=6, spaceAfter=3, textColor=colors.HexColor("#1f3a5f"))
T = ParagraphStyle("T", parent=B, fontSize=8.4, leading=10.6, spaceAfter=0)
CAP = ParagraphStyle("CAP", parent=B, fontSize=8.4, leading=10.5, textColor=colors.HexColor("#444444"), spaceAfter=7)
TITLE = ParagraphStyle("TITLE", parent=ss["Title"], fontName="Helvetica-Bold", fontSize=18, leading=22, alignment=0, spaceAfter=2)
def P(t, s=B): return Paragraph(t, s)
def bullets(items): return [Paragraph("&bull; " + i, ParagraphStyle("bl", parent=B, leftIndent=12, firstLineIndent=-8, spaceAfter=2.5)) for i in items]

def table(rows, widths, head=True):
    t = Table([[Paragraph(str(c), T) for c in r] for r in rows], colWidths=widths, repeatRows=1 if head else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#bbbbbb")), ("VALIGN", (0, 0), (-1, -1), "TOP"),
          ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5)]
    if head: st.append(("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8eef6")))
    t.setStyle(TableStyle(st)); return t

def chart(fname, width_cm, caption):
    p = os.path.join(OUT, fname)
    if not os.path.exists(p): return [P(f"[missing {fname}]")]
    from PIL import Image as PI
    w, h = PI.open(p).size
    return [KeepTogether([Image(p, width=width_cm * cm, height=width_cm * cm * h / w), P(caption, CAP)])]

def arch():
    d = Drawing(600, 280)
    def box(x, y, w, h, txt, fill, sub=None):
        d.add(Rect(x, y, w, h, rx=4, ry=4, fillColor=colors.HexColor(fill), strokeColor=colors.HexColor("#5a6b80"), strokeWidth=0.8))
        d.add(String(x + w / 2, y + h - 11, txt, textAnchor="middle", fontName="Helvetica-Bold", fontSize=8, fillColor=colors.HexColor("#102030")))
        for i, s in enumerate(sub or []):
            d.add(String(x + w / 2, y + h - 22 - 9 * i, s, textAnchor="middle", fontName="Helvetica", fontSize=6.8, fillColor=colors.HexColor("#33414f")))
    def arrow(x1, y1, x2, y2):
        d.add(Line(x1, y1, x2, y2, strokeColor=colors.HexColor("#5a6b80"), strokeWidth=1))
        d.add(Polygon([x2, y2, x2 - 6, y2 + 3, x2 - 6, y2 - 3], fillColor=colors.HexColor("#5a6b80"), strokeColor=None))
    box(5, 125, 95, 50, "Generator", "#fff3d6", ["Faker + numpy, seed 42", "JSON Lines files"])
    box(150, 215, 130, 50, "MongoDB", "#dfeefb", ["users, products,", "transactions, sessions"])
    box(150, 125, 130, 50, "HBase", "#e1f4ea", ["user_sessions,", "product_metrics"])
    box(150, 35, 130, 50, "Parquet (by day)", "#f0e6f7", ["cleaned, typed, partitioned"])
    box(330, 125, 130, 50, "Apache Spark", "#ffe3d6", ["clean, join, SQL,", "window functions"])
    box(500, 125, 90, 50, "Outputs", "#eeeeee", ["CSV + 4 PNG charts", "report"])
    for yy in (240, 150, 60): arrow(100, 150, 150, yy)
    arrow(280, 240, 330, 160); arrow(280, 150, 330, 150); arrow(280, 60, 330, 140); arrow(460, 150, 500, 150)
    d.width, d.height = 600 * 0.8, 280 * 0.8
    d.scale(0.8, 0.8)
    return d

doc = SimpleDocTemplate(PDF, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=1.8 * cm, bottomMargin=1.7 * cm,
                        title="Technical Report - Distributed Multi-Model E-Commerce Analytics", author=AUTHOR)
S = []
S += [P("Distributed Multi-Model E-Commerce Analytics", TITLE),
      P("MongoDB, HBase and Apache Spark &mdash; Technical Report", H2),
      P(f"{AUTHOR} &middot; {STUDENT_ID} &middot; AUCA Big Data Analytics, individual final project &middot; Repository: {REPO}", T), Spacer(1, 6)]

S += [P("1. Overview and dataset", H1),
      P("The project builds an analytics platform for a synthetic e-commerce business. Browsing behaviour and purchases are "
        "stored in two NoSQL systems chosen for their access patterns, cleaned and analysed with Apache Spark, and combined in an "
        "integrated funnel analysis that no single system could answer alone. All code, Docker setup and exact reproduction "
        "steps are in the repository README."),
      P("A seeded generator (seed 42) produces 10,000 users, 5,000 products in 25 categories, 50,000 transactions and 200,000 "
        "browsing sessions as JSON Lines. A validator checks referential integrity and totals; it found the session "
        "<i>converted</i> flag inconsistent with the transactions (40,607 sessions flagged converted but only 25,204 "
        "transactions linked to a session), so in this report <b>&ldquo;purchased&rdquo; means a transaction record exists</b>.")]

S += [P("2. Architecture and technology justification", H1), arch(),
      P("Figure 1. Data flow: the generator writes JSON Lines; the same data is loaded into MongoDB and HBase (each modelled for its "
        "own queries) and into Parquet for Spark; the integration step reads HBase and MongoDB into Spark.", CAP),
      table([["System", "Used for", "Why this system"],
             ["MongoDB", "Users, product catalogue, orders, sessions", "Nested, attribute-rich documents; embedded items and category avoid joins; the aggregation framework answers revenue and segmentation questions directly; secondary indexes serve lookups by user and product."],
             ["HBase", "Per-user session history, per-product daily metrics", "Very large, append-only, time-ordered events. One row-key design makes &ldquo;latest N sessions of a user&rdquo; a short prefix scan, with no secondary index."],
             ["Spark", "Cleaning, joins, analytics at scale", "Distributed joins and window functions over data that neither store aggregates well; reads Parquet, MongoDB and HBase extracts; scales out by adding executors."]],
            [2.3 * cm, 4.2 * cm, 10.5 * cm])]

S += [P("3. Part 1 &mdash; Data modelling and storage", H1), P("3.1 MongoDB document model", H2),
      *bullets(["<b>_id = business key</b> (user_id, product_id, transaction_id, session_id): no extra id field, free uniqueness.",
                "<b>Embedding</b> where data is read together and bounded: category and subcategory inside products, line items inside transactions, "
                "page views inside sessions (the cart dictionary with dynamic product keys is converted to an array so it can be indexed), "
                "and a pre-aggregated <i>purchase_summary</i> inside each user (total orders and spend, refreshed with bulk updates).",
                "<b>Referencing</b> only across entities that change independently (transactions refer to users and products by id).",
                "<b>Indexes:</b> compound (user_id, timestamp desc) on transactions; multikey on items.product_id and page_views.product_id."]),
      P("Aggregation pipelines ($unwind, $lookup, $group, $bucket, $dateToString) produce revenue by category and month, user segments by purchase "
        "frequency, units sold per product and views per product. Revenue is the sum of item subtotals, before the order-level discount."),
      table([["Query: latest 5 transactions of one user", "Plan", "Docs examined", "Time"],
             ["Without the compound index (single-field timestamp index only)", "LIMIT &gt; FETCH &gt; IXSCAN (timestamp order, filtered by user)", "40,609", "83 ms"],
             ["With the compound index (user_id, timestamp desc)", "LIMIT &gt; FETCH &gt; IXSCAN", "5", "1 ms"]],
            [6.2 * cm, 5.6 * cm, 2.6 * cm, 2.6 * cm]),
      Spacer(1, 3),
      P("The compound index matches the filter and the sort, so MongoDB reads exactly the five documents it returns instead of walking the whole "
        "timestamp index &mdash; about 8,000 times fewer documents and a large reduction in latency, measured with explain(\"executionStats\")."),
      P("3.2 HBase schema", H2),
      table([["Table", "Row key", "Column families", "Access pattern served"],
             ["user_sessions", "user_id#reverse_ts, with reverse_ts = 10^13 &minus; epoch_ms (13 digits, zero-padded, UTC)", "info (device, referrer, start, status), pv (page views), cart (cart lines)", "Newest-first scan of one user's sessions; time-range scan by computing the two boundary keys; full session detail by one prefix scan."],
             ["product_metrics", "product_id#yyyymmdd", "m (views, cart_adds, purchases)", "Daily metrics of one product over a date range."]],
            [2.5 * cm, 4.8 * cm, 4.2 * cm, 5.5 * cm]),
      Spacer(1, 3),
      P("Reversing the timestamp makes HBase's ascending key order equal newest-first order, so &ldquo;last N sessions&rdquo; needs no sort. Putting the "
        "user id first keeps one user's data contiguous, and the zero-padded numeric suffix keeps lexicographic and numeric order identical. "
        "Three families separate small frequently read attributes from the bulkier event columns, so a query that only needs session summaries never reads page-view cells. "
        "A known risk is hotspotting when new users are written in sequence; at scale the user id would be salted or hashed.")]

S += [P("4. Part 2 &mdash; Spark pipelines", H1),
      *bullets(["<b>Cleaning (spark/clean.py):</b> explicit schemas (a MapType for the dynamic-key cart), type casts and timestamps, duplicates and "
                "inconsistent totals logged, Parquet partitioned by day, row counts reported at each step.",
                "<b>Bought-together analysis (spark/analysis.py):</b> transactions are exploded to items, self-joined to form product pairs, and ranked by support, "
                "confidence and lift. With 50,000 transactions, 328 pairs occur at least twice and 83,907 occur only once; high lift values on pairs seen twice are "
                "small-count artefacts, so the minimum support threshold matters more than the lift ranking on this data.",
                "<b>Spark SQL (spark/sql_queries.py):</b> (1) monthly revenue by category with a running total and month-over-month growth (window functions: SUM OVER, LAG); "
                "(2) top-3 products per category (ROW_NUMBER); (3) conversion by device and referrer; "
                "(4) users from a MongoDB-style profile table joined to HBase-style engagement and split into spend quartiles with NTILE "
                "(average spend 1,981.95 / 4,106.17 / 5,920.76 / 9,194.83; average sessions per user only 19.2 to 20.8)."]),
      P("Small dimension tables (users, purchases) are joined with broadcast joins to avoid shuffling the large session table. Across the full "
        "200,000 sessions the session-to-purchase conversion is about 12.5&ndash;13% in every device and referrer segment. The quartile result shows the "
        "synthetic generator makes spend nearly independent of browsing volume: heavy spenders do not browse noticeably more.")]

S += [P("5. Part 3 &mdash; Integrated analysis: conversion funnel across HBase, MongoDB and Spark", H1),
      P("<b>Business question.</b> Where do shoppers drop off between viewing a product and buying, and does it differ by device, referrer and customer purchase frequency?"),
      *bullets(["HBase supplies the behavioural events (page views, cart) per session &mdash; 100,000 sessions scanned in 6 seconds.",
                "MongoDB supplies the authoritative purchases (25,204 session-linked transactions) and each user's total orders (10,000 users).",
                "Spark joins the three sources (broadcast joins) and computes cumulative funnel stages: a session counts at a stage only if it passed the earlier ones."]),
      table([["Stage", "Sessions", "% of sessions", "Drop from previous stage"],
             ["All sessions", "100,000", "100%", "&ndash;"], ["Viewed a product page", "98,764", "98.8%", "1.2%"],
             ["Added to cart", "64,987", "65.0%", "34.2%"], ["Reached checkout", "28,979", "29.0%", "55.4%"],
             ["Purchased", "20,286", "20.3%", "30.0%"]], [5 * cm, 3 * cm, 3.5 * cm, 5.5 * cm]),
      Spacer(1, 3),
      P("<b>Reconciliation.</b> All 20,286 purchases matched to loaded sessions also passed every earlier stage, which confirms the join keys and stage definitions. "
        "The remaining 4,918 session-linked transactions belong to the second session file, which is not loaded in HBase. "
        "<b>Findings.</b> The largest loss is between cart and checkout (55%). Conversion is flat across devices (20.1&ndash;20.4%) and referrers (19.8&ndash;21.0%), "
        "and rises with customer order frequency from 5.1% (1 order) to 28.8% (7+ orders); because that segment is built from total orders, which include the purchase being measured, "
        "the relationship is partly circular and should be read as description, not cause.")]

S += [P("6. Engineering notes and data-quality findings", H1),
      *bullets(["<b>Generator defects found and fixed:</b> the original generator lived in a notebook file and failed outside Jupyter; sessions marked converted outnumbered "
                "session-linked transactions; sub-category ids were missing from products. The fixed generator is configurable by environment variables and writes JSON Lines.",
                "<b>Reproducibility:</b> a fixed seed, pinned layout, and a README with the full command sequence; scripts take paths as arguments and print row counts so each step can be checked against the validator.",
                "<b>HBase operations:</b> the Docker Thrift server stopped answering after long idle periods; restarting the container restores it. Scans use column filters and 2,000-row batches so the 100,000-session read finishes in seconds.",
                "<b>Spark portability:</b> on native Windows Spark needs the Hadoop native libraries; the pipeline was verified there and in WSL2/Linux, which is the documented environment.",
                "<b>Interpretation guard-rails:</b> high lift on rarely co-purchased pairs, partial first/last months, revenue before discount and subset mismatches (views from 100,000 sessions, units from all transactions) are stated wherever they affect a number."]),
      PageBreak(), P("7. Part 4 &mdash; Visualizations", H1)]
S += chart("chart1_revenue_by_category.png", 14.3, "Figure 2. Revenue by category and month. The top 5 of 25 categories make up only 23% of revenue; October 2026 is the strongest full month (August and November are partial months).")
S += chart("chart2_funnel.png", 16, "Figure 3. Integrated funnel (HBase + MongoDB). Biggest drop is cart to checkout (55% lost); overall conversion 20.3%; device and referrer differ by under 2 percentage points.")
S += chart("chart3_views_vs_units.png", 12.2, "Figure 4. Most viewed vs top selling products (5,000 products). Views are a moderate predictor of units sold (correlation 0.69); views come from the 100,000 loaded sessions while units cover all 50,000 transactions.")
S += chart("chart4_segmentation.png", 13.5, "Figure 5. Customers by purchase frequency. The 4-6 orders band is 50% of customers and 49% of revenue; the 7-10 band earns 1.6 times its customer share.")

S += [PageBreak(), P("8. Scalability, limitations and conclusion", H1), P("8.1 Scalability", H2),
      *bullets(["<b>MongoDB:</b> shard transactions by hashed user_id (even writes, user queries stay single-shard); keep sessions in a time-bucketed or archived collection; replica sets for reads and availability.",
                "<b>HBase:</b> regions split automatically; salt the row-key prefix to avoid hotspotting on new users; keep the number of column families small; set a TTL on old session cells if history can age out.",
                "<b>Spark:</b> the code uses standard DataFrame APIs, so it moves from local mode to a cluster without logic changes; partition Parquet by day, broadcast only small tables, and read HBase and MongoDB through connectors instead of the single-threaded client extracts used here."]),
      P("8.2 Limitations", H2),
      *bullets(["The data is synthetic and largely random, so behavioural signals (device, referrer, browsing volume versus spend) are weak by construction.",
                "Only the first 100,000 of 200,000 sessions are in MongoDB and HBase. Linked purchases are concentrated in that half (20,286 versus 4,918), so the funnel conversion (20.3%) is higher than the all-sessions figure from Spark SQL (about 12.5&ndash;13%); the two use different session subsets.",
                "Revenue figures are item subtotals before the order-level discount. The first and last months are partial.",
                "A single-node Docker HBase is for demonstration (its Thrift server timed out after long idle periods and needed a restart); the integration reads through the Python client and a cluster deployment would use native connectors."]),
      P("8.3 Conclusion", H2),
      P("The same retail data was modelled twice, once per access pattern, and combined where each system is weakest: MongoDB gives rich queries on orders and profiles, HBase gives "
        "fast time-ordered reads on sessions, and Spark joins them to answer a question neither could alone. The measured index effect (40,609 to 5 documents examined), the "
        "validated key design and the reconciled funnel are the main evidence for the design decisions.")]

doc.build(S, onFirstPage=lambda c, d: None, onLaterPages=lambda c, d: (c.setFont("Helvetica", 8), c.setFillColor(colors.grey), c.drawCentredString(A4[0] / 2, 1 * cm, f"Page {d.page}")))
from pypdf import PdfReader
print("saved", PDF, "pages:", len(PdfReader(PDF).pages))
