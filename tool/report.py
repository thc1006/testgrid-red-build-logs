"""Charts for the archive's INDEX.md: red builds per day, and per tab.

Every chart is written once per GitHub theme (charts/*-light.svg and
*-dark.svg); INDEX.md picks the right one with <picture>. Colors are the
dataviz reference palette: the heatmap ramps pass the ordinal checks in both
modes (light end >= 2:1 on the surface), dark mode flips the ramp's anchor,
and status hues only ever sit next to a ▲/▼ icon and a number in ink.

Two things are drawn honestly rather than hidden:
  * a day is complete only when every tab of the dashboard had TestGrid
    history covering all of it in some successful scan, after every build that
    started that day could have finished; other days are marked "partial
    history", because their counts can only be low;
  * today is faded and labelled, because the day is not over.
The 7-day average and the week-over-week change only use complete days, so a
young archive, a missed run or a tab that never loads shows no trend instead
of a false one.
"""
import datetime as dt
import glob
import json
import math
import os
import re
import tempfile
from xml.sax.saxutils import escape

TREND_DAYS = 30
HEAT_DAYS = 14
MAX_ROWS_PER_DASHBOARD = 40
FONT = "system-ui, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"

THEMES = {
    "light": {
        "surface": "#fcfcfb", "ink": "#0b0b0b", "ink2": "#52514e", "muted": "#898781",
        "grid": "#e1e0d9", "axis": "#c3c2b7", "border": "rgba(11,11,11,0.10)",
        "bar": "#5598e7", "line": "#104281", "band": "#f0efec",
        "empty": "#f0efec", "up": "#d03b3b", "down": "#006300",
        # ordinal ramp, light -> dark (validated: --ordinal --mode light)
        "ramp": ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#0d366b"],
    },
    "dark": {
        "surface": "#1a1a19", "ink": "#ffffff", "ink2": "#c3c2b7", "muted": "#898781",
        "grid": "#2c2c2a", "axis": "#383835", "border": "rgba(255,255,255,0.10)",
        "bar": "#256abf", "line": "#b7d3f6", "band": "#242422",
        "empty": "#383835", "up": "#d03b3b", "down": "#0ca30c",
        # same hue, anchor flipped for the dark surface (validated: --ordinal --mode dark)
        "ramp": ["#184f95", "#256abf", "#3987e5", "#86b6ef", "#cde2fb"],
    },
}
BINS = [(1, 1, "1"), (2, 2, "2"), (3, 4, "3–4"), (5, 8, "5–8"), (9, None, "9+")]
LABELS = {"sig-release-master-blocking": "Blocking", "sig-release-master-informing": "Informing"}


# What can start markup inside a table cell or list item: emphasis, code, links and
# images, raw HTML, a table pipe, strikethrough. Block markers only matter at the start.
MD_INLINE = re.compile(r"([\\`*_\[\]<>|!~])")


def md_cell(s):
    """Text safe inside a Markdown table cell or list item: one line, no markup, links, images or HTML."""
    s = MD_INLINE.sub(r"\\\1", re.sub(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]+", " ", str(s)).strip())
    if s and s[0] in "#+-=":
        return "\\" + s
    return re.sub(r"^(\d+)([.)])", r"\1\\\2", s)


def day_of(ts):
    return dt.datetime.fromtimestamp(ts, dt.UTC).date()


def day_start(d):
    return dt.datetime(d.year, d.month, d.day, tzinfo=dt.UTC).timestamp()


def load_runs(root, current_run=None):
    """Every readable run report (runs/<YYYY-MM>/<run>/run.json), plus the current run's."""
    runs = {}
    for path in glob.glob(os.path.join(glob.escape(root), "runs", "*", "*", "run.json")):
        try:
            with open(path, encoding="utf-8") as f:
                run = json.load(f)
            runs[run["run"]] = run
        except (OSError, ValueError, KeyError, TypeError, RecursionError):
            continue
    if current_run:
        runs[current_run["run"]] = current_run
    return [r for r in runs.values() if valid_run(r)]


def number(v, allow_none=False):
    if v is None:
        return allow_none
    try:
        return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
    except OverflowError:  # an int too large for a float
        return False


def valid_run(r):
    """A run report the coverage maths can trust; anything else is ignored."""
    return (isinstance(r, dict) and number(r.get("started")) and isinstance(r.get("tabs"), list)
            and number(r.get("since_hours"), allow_none=True) and number(r.get("max_job_hours", 6))
            and (r.get("since_hours") is None or r["since_hours"] > 0) and r.get("max_job_hours", 6) > 0)


def merge(spans):
    out = []
    for lo, hi in sorted(spans):
        if out and lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def covers(spans, lo, hi):
    return any(a <= lo and b >= hi for a, b in spans)


def overlaps(spans, lo, hi):
    return any(a < hi and b > lo for a, b in spans)


def coverage(runs):
    """Per tab, the merged time spans whose red builds the archive has seen,
    and per dashboard, the tabs its most recent summary listed.

    A successful scan at time T shows the tab's columns back to its oldest
    start O, and every build that started before T - max_job_hours has
    finished by then, so the scan covers [O, T - max_job_hours] (clipped to
    --since-hours). Tabs that failed to load cover nothing.
    """
    spans, listed = {}, {}
    for run in sorted(runs, key=lambda r: r["started"]):
        floor = run["started"] - run["since_hours"] * 3600 if run.get("since_hours") is not None else -math.inf
        seen = {}
        for t in run["tabs"]:
            if not isinstance(t, dict) or not isinstance(t.get("dashboard"), str) or not isinstance(t.get("tab"), str):
                continue
            seen.setdefault(t["dashboard"], set()).add(t["tab"])
            if t.get("error") or not number(t.get("oldest")):
                continue
            # a tab whose job can run longer than the run-wide default says so itself
            own = t.get("max_job_hours")
            hours = own if number(own) and own > 0 else run.get("max_job_hours", 6)
            lo, hi = max(t["oldest"], floor), run["started"] - hours * 3600
            if hi > lo:
                spans.setdefault((t["dashboard"], t["tab"]), []).append((lo, hi))
        listed.update(seen)
    return {k: merge(v) for k, v in spans.items()}, listed


def membership(runs):
    """Per dashboard, the tab lists its summary had over time: [(from, tabs), ...].

    A day is only complete if every tab listed during it covered it, so a tab
    that never loaded keeps its days partial even after it leaves the
    dashboard. Before the first run, the first list is assumed.
    """
    history = {}
    for run in sorted(runs, key=lambda r: r["started"]):
        seen = {}
        for t in run["tabs"]:
            if isinstance(t, dict) and isinstance(t.get("dashboard"), str) and isinstance(t.get("tab"), str):
                seen.setdefault(t["dashboard"], set()).add(t["tab"])
        for dash, tabs in seen.items():
            epochs = history.setdefault(dash, [])
            if not epochs or epochs[-1][1] != tabs:
                epochs.append((run["started"], tabs))
    return history


def tabs_during(epochs, lo, hi):
    """Every tab listed at some point in [lo, hi)."""
    out = set()
    for i, (since, tabs) in enumerate(epochs):
        until = epochs[i + 1][0] if i + 1 < len(epochs) else math.inf
        if (since if i else -math.inf) < hi and until > lo:
            out |= tabs
    return out


def summarize(entries, runs, now):
    today = day_of(now)
    days = [today - dt.timedelta(days=i) for i in range(TREND_DAYS - 1, -1, -1)]
    spans, listed = coverage(runs)
    history = membership(runs)
    dashboards = sorted(set(listed) | {t.split("#", 1)[0] for e in entries for t in e["tabs"]},
                        key=lambda d: (d not in LABELS, list(LABELS).index(d) if d in LABELS else 0, d))
    per_day = {d: {day: 0 for day in days} for d in dashboards}
    per_tab = {}
    for e in entries:
        day = day_of(e["time"])
        for dash in {t.split("#", 1)[0] for t in e["tabs"]}:
            if day in per_day.get(dash, {}):
                per_day[dash][day] += 1
        for t in set(e["tabs"]):
            dash, tab = t.split("#", 1)
            per_tab.setdefault((dash, tab), {})
            per_tab[(dash, tab)][day] = per_tab[(dash, tab)].get(day, 0) + 1

    panels = []
    for dash in dashboards:
        complete = set()
        for day in days:
            lo, hi = day_start(day), day_start(day) + 86400
            tabs = tabs_during(history.get(dash, []), lo, hi)
            if day < today and tabs and all(covers(spans.get((dash, t), []), lo, hi) for t in tabs):
                complete.add(day)
        counts = per_day[dash]
        avg = {}
        for i, day in enumerate(days):
            window = days[max(0, i - 6):i + 1]
            if len(window) == 7 and all(w in complete for w in window):
                avg[day] = sum(counts[w] for w in window) / 7
        # Compare the last 7 complete days with the 7 before. Yesterday is complete
        # only once a run has scanned long enough after midnight, so the window may
        # end the day before; it never jumps between two answers within a day.
        week = week_end = None
        for back in (1, 2):
            end = today - dt.timedelta(days=back)
            last7 = [end - dt.timedelta(days=i) for i in range(7)]
            prev7 = [end - dt.timedelta(days=i) for i in range(7, 14)]
            if all(d in complete for d in last7 + prev7) and all(d in counts for d in last7 + prev7):
                week, week_end = (sum(counts[d] for d in last7), sum(counts[d] for d in prev7)), end
                break
        panels.append({"dashboard": dash, "counts": counts, "complete": complete, "avg": avg, "week": week,
                       "week_end": week_end})

    heat_days = days[-HEAT_DAYS:]
    rows = []
    for (dash, tab), counts in per_tab.items():
        in_window = {d: c for d, c in counts.items() if d in heat_days}
        if not in_window:
            continue
        rows.append({"dashboard": dash, "tab": tab, "counts": in_window, "spans": spans.get((dash, tab), []),
                     "last7": sum(c for d, c in in_window.items() if d > today - dt.timedelta(days=7))})
    order = {d: i for i, d in enumerate(dashboards)}
    rows.sort(key=lambda r: (order.get(r["dashboard"], 99), -r["last7"], -sum(r["counts"].values()), r["tab"]))
    return {"today": today, "days": days, "heat_days": heat_days, "panels": panels, "rows": rows}


def nice_scale(peak):
    for step in (1, 2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000):
        if math.ceil(max(peak, 1) / step) <= 4:
            top = max(step, math.ceil(peak / step) * step)
            return top, list(range(0, top + 1, step))
    top = math.ceil(peak / 1000) * 1000
    return top, [0, top // 2, top]


def xml_text(s):
    """Escaped text without the characters XML 1.0 forbids, even escaped (control
    characters, and lone surrogates from a JSON escape)."""
    return escape(re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]", " ", str(s)))


def text(x, y, s, size=12, fill="#000", weight=400, anchor="start", extra=""):
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" fill="{fill}" '
            f'text-anchor="{anchor}" {extra}>{xml_text(s)}</text>')


def column(x, y, w, h, r, fill, opacity=1.0):
    """A column with a rounded data end and a square baseline end."""
    r = min(r, w / 2, h)
    op = f' fill-opacity="{opacity}"' if opacity < 1 else ""
    return (f'<path d="M{x:.1f},{y + h:.1f} V{y + r:.1f} Q{x:.1f},{y:.1f} {x + r:.1f},{y:.1f} '
            f'H{x + w - r:.1f} Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + r:.1f} V{y + h:.1f} Z" fill="{fill}"{op}/>')


def svg(width, height, body, c, title):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" font-family="{FONT}" role="img">'
            f'<title>{xml_text(title)}</title>'
            f'<rect x="0.5" y="0.5" width="{width - 1}" height="{height - 1}" rx="8" fill="{c["surface"]}" '
            f'stroke="{c["border"]}"/>' + "".join(body) + "</svg>\n")


def week_text(week):
    if week is None:
        return None, "week-over-week change appears after 14 fully covered days"
    now, before = week
    if now == before:
        change = "no change"
    elif before == 0:
        change = "new"
    else:
        pct = 100 * (now - before) / before
        if abs(pct) < 1:
            change = "+<1%" if pct > 0 else "-<1%"
        elif now and round(pct) == -100:
            change = f"{pct:+.1f}%"  # not "-100%" while a few builds remain
        else:
            change = f"{round(pct):+d}%"
    arrow = "up" if now > before else "down" if now < before else None
    return arrow, f"last 7 full days {now} · {change} vs previous 7 ({before})"


def render_trend(s, mode):
    c = THEMES[mode]
    W, left, right, top = 880, 52, 132, 96
    plot_h, axis_h, gap = 120, 26, 34
    n = len(s["days"])
    pw = W - left - right
    slot = pw / n
    bw = min(24, slot * 0.68)
    peak = max([max(p["counts"].values(), default=0) for p in s["panels"]] + [1])
    ymax, ticks = nice_scale(peak)
    H = top + len(s["panels"]) * (plot_h + axis_h + gap + 24) + 8
    b = [text(24, 34, "Red builds per day", 17, c["ink"], 600),
         text(24, 56, "UTC days · a build counts once per dashboard on the day it started", 12, c["ink2"])]
    # legend: mirrors the marks (rect for bars, line for the line)
    lx = 24
    b.append(f'<rect x="{lx}" y="68" width="12" height="12" rx="2" fill="{c["bar"]}"/>')
    b.append(text(lx + 18, 78.5, "red builds", 12, c["ink2"]))
    lx += 104
    b.append(f'<line x1="{lx}" y1="74" x2="{lx + 18}" y2="74" stroke="{c["line"]}" stroke-width="2" stroke-linecap="round"/>')
    b.append(text(lx + 24, 78.5, "7-day average", 12, c["ink2"]))
    lx += 124
    b.append(f'<rect x="{lx}" y="68" width="12" height="12" rx="2" fill="{c["band"]}" stroke="{c["axis"]}"/>')
    b.append(text(lx + 18, 78.5, "partial history (counts may be low)", 12, c["ink2"]))
    lx += 250
    b.append(f'<rect x="{lx}" y="68" width="12" height="12" rx="2" fill="{c["bar"]}" fill-opacity="0.5"/>')
    b.append(text(lx + 18, 78.5, "today so far", 12, c["ink2"]))

    y0 = top
    for p in s["panels"]:
        name = LABELS.get(p["dashboard"], p["dashboard"])
        b.append(text(24, y0 + 14, name, 14, c["ink"], 600))
        b.append(text(24 + 9 * len(name) + 10, y0 + 14, p["dashboard"], 12, c["muted"]))
        arrow, wt = week_text(p["week"])
        x_end = W - 24
        b.append(text(x_end, y0 + 14, wt, 12, c["ink"] if arrow else c["muted"], 400, "end"))
        if arrow:
            ax = x_end - 7.2 * len(wt) - 14
            pts = (f"{ax},{y0 + 4} {ax + 5},{y0 + 13} {ax - 5},{y0 + 13}" if arrow == "up"
                   else f"{ax},{y0 + 13} {ax + 5},{y0 + 4} {ax - 5},{y0 + 4}")
            b.append(f'<polygon points="{pts}" fill="{c["up" if arrow == "up" else "down"]}"/>')
        py = y0 + 26
        base = py + plot_h
        for t in ticks:
            yy = base - plot_h * t / ymax
            b.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{left + pw}" y2="{yy:.1f}" '
                     f'stroke="{c["axis"] if t == 0 else c["grid"]}" stroke-width="1"/>')
            b.append(text(left - 8, yy + 4, f"{t:,}", 11, c["muted"], 400, "end", 'font-variant-numeric="tabular-nums"'))
        partial = [i for i, d in enumerate(s["days"]) if d not in p["complete"] and d != s["today"]]
        runs = []  # contiguous stretches of partial days, one band each
        for i in partial:
            if runs and runs[-1][1] == i - 1:
                runs[-1][1] = i
            else:
                runs.append([i, i])
        for first, last in runs:
            x1, x2 = left + first * slot, left + (last + 1) * slot
            b.append(f'<rect x="{x1:.1f}" y="{py:.1f}" width="{x2 - x1:.1f}" height="{plot_h:.1f}" fill="{c["band"]}"/>')
            if x2 - x1 > 200:
                b.append(text((x1 + x2) / 2, py + 14, "partial history · counts may be low", 11, c["muted"], 400, "middle"))
            elif x2 - x1 > 60:
                b.append(text((x1 + x2) / 2, py + 14, "partial", 11, c["muted"], 400, "middle"))
        top_i = max(range(n), key=lambda i: (p["counts"][s["days"][i]], i))
        for i, d in enumerate(s["days"]):
            v = p["counts"][d]
            if not v:
                continue
            h = plot_h * v / ymax
            x = left + i * slot + (slot - bw) / 2
            b.append(column(x, base - h, bw, h, 4, c["bar"], 0.5 if d == s["today"] else 1.0))
            if i == top_i:
                b.append(text(x + bw / 2, base - h - 6, str(v), 11, c["ink2"], 600, "middle"))
        # one line per stretch of days with an average: never bridge days without one
        segments = []
        for i, d in enumerate(s["days"]):
            if d not in p["avg"]:
                continue
            point = (left + i * slot + slot / 2, base - plot_h * p["avg"][d] / ymax)
            if segments and segments[-1][-1][0] == i - 1:
                segments[-1].append((i, point))
            else:
                segments.append([(i, point)])
        if segments:
            for seg in segments:
                if len(seg) == 1:  # a lone day: a dot, since a one-point line draws nothing
                    x, y = seg[0][1]
                    b.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.5" fill="{c["line"]}"/>')
                    continue
                path = " ".join(f"{x:.1f},{y:.1f}" for _, (x, y) in seg)
                b.append(f'<polyline points="{path}" fill="none" stroke="{c["surface"]}" stroke-width="5" '
                         'stroke-linejoin="round" stroke-linecap="round"/>')
                b.append(f'<polyline points="{path}" fill="none" stroke="{c["line"]}" stroke-width="2" '
                         'stroke-linejoin="round" stroke-linecap="round"/>')
            ex, ey = segments[-1][-1][1]
            b.append(f'<circle cx="{ex:.1f}" cy="{ey:.1f}" r="4" fill="{c["line"]}" stroke="{c["surface"]}" stroke-width="2"/>')
            last_avg = p["avg"][max(p["avg"])]
            b.append(text(left + pw + 10, ey + 4, f"avg {last_avg:.1f}/day", 12, c["ink"], 600))
        else:
            b.append(text(left + pw + 10, base - plot_h / 2, "7-day average:", 11, c["muted"]))
            b.append(text(left + pw + 10, base - plot_h / 2 + 15, "needs 7 full days", 11, c["muted"]))
        for i, d in enumerate(s["days"]):
            if d == s["today"]:
                label = "today"
            elif (n - 1 - i) % 7 == 0:
                label = f"{d:%b} {d.day}"
            else:
                continue
            b.append(text(left + i * slot + slot / 2, base + 16, label, 11, c["muted"], 400, "middle"))
        y0 = base + axis_h + gap
    return svg(W, H, b, c, "Red builds per day")


def luminance(hex_color):
    def lin(v):
        v /= 255
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4
    r, g, bl = (int(hex_color[i:i + 2], 16) for i in (1, 3, 5))
    return 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(bl)


def ink_on(fill):
    # pick whichever of white / near-black contrasts more with the fill
    lum = luminance(fill)
    return "#ffffff" if (1.05 / (lum + 0.05)) >= ((lum + 0.05) / 0.0504) else "#0b0b0b"


def bin_color(v, c):
    for i, (lo, hi, _) in enumerate(BINS):
        if v >= lo and (hi is None or v <= hi):
            return c["ramp"][i]
    return c["ramp"][-1]


def render_heatmap(s, mode):
    c = THEMES[mode]
    W, left, label_w, total_w = 880, 24, 300, 74
    days = s["heat_days"]
    cell_w = (W - left - label_w - total_w - 24) / len(days)
    cell_h, row_gap = 22, 2
    groups = {}
    for r in s["rows"]:
        groups.setdefault(r["dashboard"], []).append(r)
    top = 112
    gx = left + label_w
    b = [text(24, 34, f"Red builds per tab, last {len(days)} days", 17, c["ink"], 600),
         text(24, 56, "Rows: tabs with a red build in the window, busiest last 7 days first · "
              "number = red builds started that day", 12, c["ink2"])]
    if not s["rows"]:
        b.append(text(24, 92, "No red builds in this window.", 13, c["ink2"]))
        return svg(W, 120, b, c, "Red builds per tab")
    for i, d in enumerate(days):
        x = gx + i * cell_w + cell_w / 2
        label = "today" if d == s["today"] else (f"{d:%b} {d.day}" if i == 0 or d.day == 1 else str(d.day))
        b.append(text(x, top - 12, label, 11, c["ink"] if d == s["today"] else c["muted"], 400, "middle"))
    b.append(text(W - 24, top - 26, "7 days", 11, c["muted"], 400, "end"))
    b.append(text(W - 24, top - 12, "incl. today", 11, c["muted"], 400, "end"))
    y = top
    for dash, rows in groups.items():
        b.append(text(24, y + 16, LABELS.get(dash, dash), 13, c["ink"], 600))
        y += 26
        for r in rows[:MAX_ROWS_PER_DASHBOARD]:
            name = r["tab"] if len(r["tab"]) <= 44 else r["tab"][:43] + "…"
            b.append(text(left, y + 15, name, 12, c["ink"]))
            for i, d in enumerate(days):
                x = gx + i * cell_w + 1
                v = r["counts"].get(d, 0)
                if not v and not overlaps(r["spans"], day_start(d), day_start(d) + 86400):
                    # no TestGrid history for this tab that day: outline only
                    b.append(f'<rect x="{x + 0.5:.1f}" y="{y + 0.5:.1f}" width="{cell_w - 3:.1f}" height="{cell_h - 1}" '
                             f'rx="3" fill="none" stroke="{c["grid"]}"/>')
                    continue
                fill = bin_color(v, c) if v else c["empty"]
                b.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{cell_w - 2:.1f}" height="{cell_h}" rx="3" fill="{fill}"/>')
                if v:
                    b.append(text(x + (cell_w - 2) / 2, y + 15, str(v), 11, ink_on(fill), 600, "middle"))
            b.append(text(W - 24, y + 15, str(r["last7"]), 12, c["ink"], 600, "end", 'font-variant-numeric="tabular-nums"'))
            y += cell_h + row_gap
        if len(rows) > MAX_ROWS_PER_DASHBOARD:
            more = len(rows) - MAX_ROWS_PER_DASHBOARD
            b.append(text(left, y + 13, f"+{more} more tabs (see the per-tab table)", 11, c["muted"]))
            y += 18
        y += 4
    # legend: every class, including the two non-values
    y += 18
    x = left
    b.append(f'<rect x="{x + 0.5}" y="{y + 0.5}" width="13" height="13" rx="3" fill="none" stroke="{c["grid"]}"/>')
    b.append(text(x + 20, y + 11.5, "no history yet", 11, c["ink2"]))
    x += 112
    b.append(f'<rect x="{x}" y="{y}" width="14" height="14" rx="3" fill="{c["empty"]}"/>')
    b.append(text(x + 20, y + 11.5, "0", 11, c["ink2"]))
    x += 44
    for i, (_, _, label) in enumerate(BINS):
        b.append(f'<rect x="{x}" y="{y}" width="14" height="14" rx="3" fill="{c["ramp"][i]}"/>')
        b.append(text(x + 20, y + 11.5, label, 11, c["ink2"]))
        x += 30 + 7 * len(label)
    b.append(text(x + 6, y + 11.5, "red builds per day", 11, c["muted"]))
    return svg(W, y + 34, b, c, "Red builds per tab")


def picture(name, alt):
    return (f'<picture>\n  <source media="(prefers-color-scheme: dark)" srcset="charts/{name}-dark.svg">\n'
            f'  <img alt="{escape(" ".join(alt.split()), {chr(34): "&quot;"})}" src="charts/{name}-light.svg">\n</picture>')


def write_charts(root, entries, now, current_run=None):
    """Write charts/*.svg and return the Markdown that shows them."""
    s = summarize(entries, load_runs(root, current_run), now)
    os.makedirs(os.path.join(root, "charts"), exist_ok=True)
    for mode in THEMES:
        for name, render in (("trend", render_trend), ("tabs", render_heatmap)):
            path = os.path.join(root, "charts", f"{name}-{mode}.svg")
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=f".{name}-{mode}.svg.", suffix=".part")
            try:
                with os.fdopen(fd, "w", encoding="utf-8", errors="backslashreplace") as f:
                    os.fchmod(f.fileno(), 0o644)
                    f.write(render(s, mode))
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
    headline = []
    for p in s["panels"]:
        _, wt = week_text(p["week"])
        headline.append(f"{LABELS.get(p['dashboard'], p['dashboard'])}: {wt}")
    busiest = sorted(s["rows"], key=lambda r: -r["last7"])[:5]
    tabs_alt = (f"Red builds per tab, last {HEAT_DAYS} days. Busiest over the 7 days including today: "
                + ", ".join(f"{r['tab']} {r['last7']}" for r in busiest if r["last7"]) + ".")
    lines = [picture("trend", "Red builds per day. " + "; ".join(headline)), "",
             picture("tabs", tabs_alt), "",
             "<details><summary>Red builds per tab (table)</summary>", "",
             "| Dashboard | Tab | 7 days incl. today | " + f"{HEAT_DAYS} days |", "|---|---|---:|---:|"]
    for r in s["rows"]:
        lines.append(f"| {md_cell(LABELS.get(r['dashboard'], r['dashboard']))} | {md_cell(r['tab'])} | "
                     f"{r['last7']} | {sum(r['counts'].values())} |")
    lines += ["", "</details>", "",
              "<details><summary>Daily counts (table)</summary>", "",
             "| Day (UTC) | " + " | ".join(LABELS.get(p["dashboard"], p["dashboard"]) for p in s["panels"]) + " |",
             "|---|" + "---:|" * len(s["panels"])]
    for d in reversed(s["days"]):
        cells = []
        for p in s["panels"]:
            v = str(p["counts"][d])
            if d == s["today"]:
                v += " (today so far)"
            elif d not in p["complete"]:
                v += " (partial)"
            cells.append(v)
        lines.append(f"| {d.isoformat()} | " + " | ".join(cells) + " |")
    lines += ["", "</details>", ""]
    return lines
