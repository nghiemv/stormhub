import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pystac

# -----------------------------
# CONFIG
# -----------------------------

RP_THRESHOLD_YEARS = 2.0

# Plot headroom: extend y-axis so labels fit
MAX_RP_PLOT_YEARS = 500  # "data cap" reference
Y_HEADROOM_MULT = 1.8  # extra headroom for labels on log axis

EXPORT_BASENAME = "calibration_candidates"
PLOT_DIRNAME = "calibration_candidates"


# -----------------------------
# DUCKDB SCHEMA (minimal)
# -----------------------------
SQL_CREATE_AMS_TABLE = """
CREATE TABLE ams (
    gage_id VARCHAR,
    station_name VARCHAR,
    date DATE,
    peak_va DOUBLE
)
"""

SQL_INSERT_FROM_TEMP = """
INSERT INTO ams
SELECT gage_id, station_name, date, peak_va
FROM temp_df
"""

SQL_COUNT_RECORDS = "SELECT COUNT(*) FROM ams"


# -----------------------------
# HELPERS
# -----------------------------
def human_flow(x: float) -> str:
    """
    Human-readable flow like 5.1k, 2.3M.
    """
    if not np.isfinite(x):
        return ""
    x = float(x)
    ax = abs(x)
    if ax >= 1_000_000:
        return f"{x/1_000_000:.1f}M"
    if ax >= 1_000:
        return f"{x/1_000:.1f}k"
    return f"{x:.0f}"


def prepare_dataframe(df: pd.DataFrame, gage_id: str, station_name: str) -> pd.DataFrame:
    df = df.reset_index()

    if "datetime" in df.columns:
        df = df.rename(columns={"datetime": "date"})
    elif "index" in df.columns:
        df = df.rename(columns={"index": "date"})

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["gage_id"] = str(gage_id)
    df["station_name"] = station_name or ""

    out = df[["gage_id", "station_name", "date", "peak_va"]].copy()
    out = out[(out["peak_va"].notna()) & (out["peak_va"] > 0)]
    return out


def load_gages_data(
    conn: duckdb.DuckDBPyConnection, collection: pystac.Collection
) -> Tuple[int, Dict[str, Dict[str, pd.Timestamp]]]:
    """
    Loads AMS into DuckDB + returns gage count and time series availability ranges.

    IMPORTANT: deterministic ordering by sorting items by gage_id.
    """
    conn.execute(SQL_CREATE_AMS_TABLE)

    # deterministic: sort items
    items = sorted(list(collection.get_items()), key=lambda it: str(it.id))

    count = 0
    gage_availability: Dict[str, Dict[str, pd.Timestamp]] = {}

    for item in items:
        if "annual_maxima_series" not in item.assets:
            continue

        gage_id = str(item.id)
        station_name = item.properties.get("station_nm", "") or ""

        start_dt = item.properties.get("start_datetime")
        end_dt = item.properties.get("end_datetime")
        if start_dt and end_dt:
            gage_availability[gage_id] = {
                "start": pd.to_datetime(start_dt).date(),
                "end": pd.to_datetime(end_dt).date(),
            }

        try:
            ams_asset = item.assets["annual_maxima_series"]
            df = pd.read_parquet(ams_asset.get_absolute_href())
            df = prepare_dataframe(df, gage_id, station_name)

            conn.register("temp_df", df)
            conn.execute(SQL_INSERT_FROM_TEMP)
            count += 1
        except Exception:
            continue

    return count, gage_availability


def overlaps_window(
    availability: Optional[Dict[str, pd.Timestamp]], window_start: pd.Timestamp, window_end: pd.Timestamp
) -> bool:
    if not availability:
        return False
    a0 = availability["start"]
    a1 = availability["end"]
    s = pd.to_datetime(window_start).date()
    e = pd.to_datetime(window_end).date()
    return (a0 <= e) and (a1 >= s)


def get_return_period_for_flow(flow_cfs: float, rp_table: List[dict]) -> float:
    """
    Log-log interpolate return period from rp_table points.
    Expects:
      - discharge_CFS_(Approximate)
      - return_period
    """
    if not rp_table or not np.isfinite(flow_cfs) or flow_cfs <= 0:
        return np.nan

    pts = []
    for d in rp_table:
        q = d.get("discharge_CFS_(Approximate)", np.nan)
        rp = d.get("return_period", np.nan)
        try:
            q = float(q)
            rp = float(rp)
        except Exception:
            continue
        if np.isfinite(q) and np.isfinite(rp) and q > 0 and rp > 0:
            pts.append((q, rp))

    if len(pts) < 2:
        return np.nan

    pts.sort(key=lambda x: x[0])
    q = np.array([p[0] for p in pts], dtype=float)
    rp = np.array([p[1] for p in pts], dtype=float)

    if flow_cfs <= q[0]:
        return 1.0
    if flow_cfs >= q[-1]:
        return float(rp[-1])

    return float(10 ** np.interp(np.log10(flow_cfs), np.log10(q), np.log10(rp)))


def preload_rp_tables(collection: pystac.Collection) -> Dict[str, List[dict]]:
    """
    Deterministic: sort items by gage_id.
    """
    items = sorted(list(collection.get_items()), key=lambda it: str(it.id))
    rp_tables: Dict[str, List[dict]] = {}
    for item in items:
        gage_id = str(item.id)
        if "annual_maxima_series" not in item.assets:
            continue
        rp_tables[gage_id] = item.assets["annual_maxima_series"].extra_fields.get("file:values", []) or []
    return rp_tables


# -----------------------------
# WINDOW DETAILS
# -----------------------------
def compute_window_details(
    conn: duckdb.DuckDBPyConnection,
    rp_tables: Dict[str, List[dict]],
    gage_availability: Dict[str, Dict[str, pd.Timestamp]],
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> pd.DataFrame:
    """
    For a window:
      - per gage: max peak flow in window + date it occurred (peak_date)
      - return period
      - time series availability (overlap)
    """
    s = pd.to_datetime(window_start).date().isoformat()
    e = pd.to_datetime(window_end).date().isoformat()

    # deterministic tie-break for peak_date when multiple rows have same peak_va:
    # we pick MIN(date) via aggregation.
    sql = f"""
    WITH w AS (
      SELECT gage_id, station_name, date, peak_va
      FROM ams
      WHERE date BETWEEN '{s}' AND '{e}'
    ),
    mx AS (
      SELECT gage_id, MAX(peak_va) AS peak_flow_cfs
      FROM w
      GROUP BY gage_id
    ),
    pick AS (
      SELECT
        w.gage_id,
        ANY_VALUE(w.station_name) AS station_name,
        mx.peak_flow_cfs,
        MIN(w.date) AS peak_date
      FROM mx
      JOIN w
        ON w.gage_id = mx.gage_id
       AND w.peak_va = mx.peak_flow_cfs
      GROUP BY w.gage_id, mx.peak_flow_cfs
    )
    SELECT * FROM pick
    """
    base = conn.execute(sql).df()
    if base.empty:
        return base

    rows = []
    for _, r in base.iterrows():
        gage_id = str(r["gage_id"])
        station_name = r.get("station_name", "") or ""
        flow = float(r["peak_flow_cfs"])
        peak_date = pd.to_datetime(r["peak_date"]).date()

        rp = get_return_period_for_flow(flow, rp_tables.get(gage_id, []))
        has_ts = overlaps_window(gage_availability.get(gage_id), window_start, window_end)

        rows.append(
            {
                "window_start": pd.to_datetime(window_start).date(),
                "window_end": pd.to_datetime(window_end).date(),
                "gage_id": gage_id,
                "station_name": station_name,
                "peak_date": peak_date,
                "peak_flow_cfs": flow,
                "return_period_yrs": rp,
                "has_timeseries": bool(has_ts),
            }
        )

    out = pd.DataFrame(rows)
    out = out[np.isfinite(out["return_period_yrs"])].copy()
    return out


# -----------------------------
# PER-YEAR BEST WINDOW (IDEMPOTENT)
# -----------------------------
def select_best_window_per_year(
    conn: duckdb.DuckDBPyConnection,
    rp_tables: Dict[str, List[dict]],
    gage_availability: Dict[str, Dict[str, pd.Timestamp]],
    tol_days: int,
) -> Tuple[pd.DataFrame, Dict[int, pd.DataFrame]]:
    """
    For each year:
      - consider anchors = AMS dates in that year
      - pick the single best ±tol window that maximizes #gages with RP>2

    Idempotent tie-break:
      key = (num_gages, max_rp, avg_rp, max_flow, -anchor_date?) but we want earliest anchor on ties,
      so we use:
      key = (num_gages, max_rp, avg_rp, max_flow, -anchor_ord) is messy; instead compare:
        primary tuple = (num_gages, max_rp, avg_rp, max_flow)
        then tie-break by earliest anchor_date.
    We also round floats used in comparisons to avoid tiny floating noise.
    """
    years = conn.execute("SELECT DISTINCT CAST(EXTRACT(YEAR FROM date) AS INTEGER) AS yr FROM ams ORDER BY yr").df()
    if years.empty:
        raise RuntimeError("No AMS data loaded.")

    year_rows = []
    year_to_detail: Dict[int, pd.DataFrame] = {}

    for yr in years["yr"].astype(int).tolist():
        anchors = conn.execute(
            f"""
            SELECT DISTINCT date AS anchor_date
            FROM ams
            WHERE CAST(EXTRACT(YEAR FROM date) AS INTEGER) = {yr}
            ORDER BY anchor_date
            """
        ).df()

        best_primary = None
        best_anchor = None
        best_row = None
        best_detail = None

        for _, a in anchors.iterrows():
            anchor = pd.to_datetime(a["anchor_date"])
            window_start = anchor - pd.Timedelta(days=tol_days)
            window_end = anchor + pd.Timedelta(days=tol_days)

            detail = compute_window_details(conn, rp_tables, gage_availability, window_start, window_end)
            if detail.empty:
                continue

            det2 = detail[detail["return_period_yrs"] > RP_THRESHOLD_YEARS].copy()
            if det2.empty:
                continue

            num_g = int(det2["gage_id"].nunique())
            max_rp = float(det2["return_period_yrs"].max())
            avg_rp = float(det2["return_period_yrs"].mean())
            max_flow = float(det2["peak_flow_cfs"].max())

            # Round for stable comparisons
            primary = (num_g, round(max_rp, 6), round(avg_rp, 6), round(max_flow, 6))

            if best_primary is None:
                take = True
            elif primary > best_primary:
                take = True
            elif primary == best_primary:
                # deterministic tie-break: earliest anchor_date wins
                take = anchor.date() < best_anchor
            else:
                take = False

            if take:
                det2 = det2.sort_values(["peak_date", "gage_id"]).reset_index(drop=True)
                best_primary = primary
                best_anchor = anchor.date()
                best_row = {
                    "year": yr,
                    "anchor_date": anchor.date(),
                    "window_start": window_start.date(),
                    "window_end": window_end.date(),
                    "num_gages_rp_gt_2yr": num_g,
                    "max_rp_yrs": max_rp,
                    "avg_rp_yrs": avg_rp,
                    "max_peak_flow_cfs": max_flow,
                }
                best_detail = det2

        if best_row is not None and best_detail is not None:
            year_rows.append(best_row)
            year_to_detail[yr] = best_detail

    if not year_rows:
        raise RuntimeError("No yearly windows contained any gages with RP > 2 years.")

    summary = pd.DataFrame(year_rows)
    summary = summary.sort_values(
        ["num_gages_rp_gt_2yr", "max_rp_yrs", "avg_rp_yrs", "max_peak_flow_cfs", "year"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)

    return summary, year_to_detail


# -----------------------------
# PLOTTING
# -----------------------------
def plot_year_event(detail: pd.DataFrame, year: int, rank: int, out_dir: Path) -> Path:
    """
    One plot per year (best window).

    - Bars = return period (log y-axis)
    - Bottom x-axis = gage_id
    - Top x-axis = peak_date (true axis, not data-anchored text)
    - Flow label inside bar near bottom: '<flow> (cfs)'
    - Color = red if has_timeseries else gray
    - Assumes detail already sorted by peak_date (early -> late)
    """
    if detail.empty:
        raise RuntimeError(f"Empty detail for year {year}")

    window_start = detail["window_start"].iloc[0]
    window_end = detail["window_end"].iloc[0]

    x = np.arange(len(detail))
    rp = detail["return_period_yrs"].astype(float).values
    flows = detail["peak_flow_cfs"].values

    colors = np.where(
        detail["has_timeseries"].values,
        "#7f8c8d",
        "#e74c3c",
    )

    # y-axis headroom
    ymax_data = np.nanmax(rp) if np.isfinite(rp).any() else RP_THRESHOLD_YEARS
    ymax = max(MAX_RP_PLOT_YEARS, ymax_data) * Y_HEADROOM_MULT

    fig, ax = plt.subplots(figsize=(max(12, 0.38 * len(detail)), 5.6))
    bars = ax.bar(x, rp, color=colors, edgecolor="black", linewidth=0.5)

    # --------------------
    # Axes & titles
    # --------------------
    ax.set_ylabel("Return Period (years)")
    ax.set_xlabel("Gage ID")
    ax.set_title(
        f"Top Year #{rank}: {window_start} to {window_end}\n"
        f"Gages with RP > {RP_THRESHOLD_YEARS:g} years | red = no time series | n={len(detail)}"
    )

    ax.set_yscale("log")
    ax.set_ylim(0.5, ymax)

    yticks = [0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000]
    yticks = [t for t in yticks if t <= ymax]
    ax.set_yticks(yticks)
    ax.set_yticklabels([str(v) if v >= 1 else f"{v:.1f}" for v in yticks])
    ax.grid(axis="y", alpha=0.25, linestyle="--")

    # --------------------
    # Bottom x-axis: gage IDs
    # --------------------
    ax.set_xticks(x)
    ax.set_xticklabels(detail["gage_id"].tolist(), rotation=45, ha="right")

    # --------------------
    # Top x-axis: peak dates (TRUE AXIS)
    # --------------------
    top_ax = ax.secondary_xaxis("top")
    top_ax.set_xticks(x)
    top_ax.set_xticklabels(
        [str(d) for d in detail["peak_date"].values],
        rotation=90,
        fontsize=8,
    )
    top_ax.set_xlabel("Peak date")

    # --------------------
    # Flow labels inside bars (near bottom)
    # --------------------
    flow_text_y = 0.8  # fixed baseline works well on log scale

    for i, (bar, flow) in enumerate(zip(bars, flows)):
        label = f"{human_flow(flow)} (cfs)"
        h = bar.get_height()

        if np.isfinite(h) and h > flow_text_y:
            ax.text(
                i,
                flow_text_y,
                label,
                ha="center",
                va="bottom",
                fontsize=7,
                rotation=90,
                clip_on=True,
            )
        else:
            # fallback for very small bars
            ax.text(
                i,
                max(0.55, h * 1.05),
                label,
                ha="center",
                va="bottom",
                fontsize=7,
                rotation=90,
                clip_on=False,
            )

    # Layout margins so nothing clips
    plt.tight_layout(rect=[0.0, 0.06, 1.0, 0.94])

    out_dir.mkdir(parents=True, exist_ok=True)
    outpath = out_dir / f"{rank:02d}_{year}_stats.png"
    plt.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)

    return outpath


from matplotlib.lines import Line2D


def _draw_geojson(ax, geojson_obj, linewidth=1.0, alpha=0.7):
    """
    Minimal GeoJSON renderer for Polygon/MultiPolygon/LineString/MultiLineString.
    Draws outlines only (no fill).
    """

    def draw_coords(coords):
        xs = [c[0] for c in coords]
        ys = [c[1] for c in coords]
        ax.plot(xs, ys, linewidth=linewidth, alpha=alpha)

    def draw_geom(geom):
        gtype = geom.get("type")
        coords = geom.get("coordinates")

        if gtype == "Polygon":
            # coords: [ring1, ring2,...], ring: [[x,y],...]
            for ring in coords:
                draw_coords(ring)

        elif gtype == "MultiPolygon":
            # coords: [polygon1, polygon2,...], polygon: [ring1, ring2,...]
            for poly in coords:
                for ring in poly:
                    draw_coords(ring)

        elif gtype == "LineString":
            draw_coords(coords)

        elif gtype == "MultiLineString":
            for line in coords:
                draw_coords(line)

        elif gtype == "GeometryCollection":
            for g in geom.get("geometries", []):
                draw_geom(g)

        # Points not needed for watershed background

    gj_type = geojson_obj.get("type")

    if gj_type == "FeatureCollection":
        for feat in geojson_obj.get("features", []):
            geom = feat.get("geometry")
            if geom:
                draw_geom(geom)
    elif gj_type == "Feature":
        geom = geojson_obj.get("geometry")
        if geom:
            draw_geom(geom)
    else:
        # raw geometry object
        draw_geom(geojson_obj)


def _rp_color_bin(rp: float) -> int:
    """
    Return an integer bin index for RP>2 classification.
    Bins (yrs): 2–5, 5–10, 10–25, 25–50, 50–100, 100+
    """
    if rp < 5:
        return 0
    if rp < 10:
        return 1
    if rp < 25:
        return 2
    if rp < 50:
        return 3
    if rp < 100:
        return 4
    return 5


def plot_year_event_map(
    detail: pd.DataFrame,
    year: int,
    rank: int,
    collection: pystac.Collection,
    out_dir: Path,
) -> Path:
    """
    Map plot for one event (year's best window).

    - Background: watershed boundary from collection asset "watershed" (GeoJSON)
    - Event gages (detail; RP > 2): colored by RP bins
    - All other gages: plotted as a separate category labeled "<2 yr"
    - Legend: colors represent return period bins; placed outside the axes

    Assumes `detail` includes columns:
      - gage_id, return_period_yrs, has_timeseries, window_start, window_end
    """

    if detail.empty:
        raise RuntimeError(f"Empty detail for year {year}")

    window_start = detail["window_start"].iloc[0]
    window_end = detail["window_end"].iloc[0]

    # ----------------------------
    # Build a lookup of ALL gage coords from STAC
    # ----------------------------
    all_rows = []
    for item in collection.get_items():
        gid = str(item.id)
        geom = item.geometry
        if not geom or geom.get("type") != "Point":
            continue
        coords = geom.get("coordinates")
        if not coords or len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        if lon is None or lat is None:
            continue
        all_rows.append({"gage_id": gid, "lon": float(lon), "lat": float(lat)})

    all_gages = pd.DataFrame(all_rows)
    if all_gages.empty:
        raise RuntimeError("No gage Point geometries found in STAC items.")

    # ----------------------------
    # Event gages (RP > 2) coords + attributes
    # ----------------------------
    event = detail.copy()
    event["gage_id"] = event["gage_id"].astype(str)

    event = event.merge(all_gages, on="gage_id", how="inner")
    if event.empty:
        raise RuntimeError(f"No event gages could be mapped (missing geometry) for year {year}")

    # ----------------------------
    # Other gages = all minus event gages
    # ----------------------------
    event_ids = set(event["gage_id"].tolist())
    other = all_gages[~all_gages["gage_id"].isin(event_ids)].copy()

    # ----------------------------
    # Load watershed GeoJSON from collection asset
    # ----------------------------
    watershed_obj = None
    ws_asset = None
    if getattr(collection, "assets", None):
        ws_asset = collection.assets.get("watershed")

    if ws_asset is not None:
        try:
            ws_path = ws_asset.get_absolute_href()
            with open(ws_path, "r") as f:
                watershed_obj = json.load(f)
        except Exception:
            watershed_obj = None  # map still works without it

    # ----------------------------
    # Color scheme for RP bins (6 bins)
    # (use simple distinct colors; you can swap these if you want)
    # ----------------------------
    bin_labels = ["2–5 yr", "5–10 yr", "10–25 yr", "25–50 yr", "50–100 yr", "100+ yr"]
    bin_colors = ["#1f77b4", "#2ca02c", "#ff7f0e", "#d62728", "#9467bd", "#8c564b"]

    event["rp_bin"] = event["return_period_yrs"].astype(float).apply(_rp_color_bin)
    event["bin_color"] = event["rp_bin"].apply(lambda i: bin_colors[int(i)])

    # Optional: distinguish timeseries availability by edge color/thickness
    # We'll keep your earlier convention: gray = has_timeseries, red = no,
    # but now colors represent RP. So we encode timeseries as marker edge:
    #   black edge = has timeseries, light edge = no.
    edge_colors = np.where(event["has_timeseries"].values, "black", "#bbbbbb")
    edge_widths = np.where(event["has_timeseries"].values, 0.9, 0.6)

    # ----------------------------
    # Plot
    # ----------------------------
    fig, ax = plt.subplots(figsize=(8.5, 8.0))

    # Watershed background (outline)
    if watershed_obj is not None:
        _draw_geojson(ax, watershed_obj, linewidth=1.0, alpha=0.55)

    # Plot other gages first (background)
    if not other.empty:
        ax.scatter(
            other["lon"].values,
            other["lat"].values,
            s=12,
            c="#dddddd",
            edgecolors="#999999",
            linewidths=0.4,
            alpha=0.65,
            zorder=2,
        )

    # Plot event gages (colored by RP bin)
    ax.scatter(
        event["lon"].values,
        event["lat"].values,
        s=55,
        c=event["bin_color"].values,
        edgecolors=edge_colors,
        linewidths=edge_widths,
        alpha=0.95,
        zorder=3,
    )

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(
        f"Event map — Top Year #{rank}: {year}\n"
        f"Window {window_start} to {window_end}\n"
        f"Colors = return period bin | marker edge: black=timeseries, gray=no | other gages shown as <2 yr"
    )
    ax.grid(alpha=0.25, linestyle="--")
    ax.set_aspect("equal", adjustable="datalim")

    # ----------------------------
    # Legend (outside, right)
    # ----------------------------
    legend_handles = []

    # RP bin color legend
    for lbl, col in zip(bin_labels, bin_colors):
        legend_handles.append(
            Line2D(
                [0],
                [0],
                marker="o",
                color="none",
                label=lbl,
                markerfacecolor=col,
                markeredgecolor="black",
                markersize=8,
            )
        )

    # Other gages category
    legend_handles.append(
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            label="<2 yr (other gages)",
            markerfacecolor="#dddddd",
            markeredgecolor="#999999",
            markersize=6,
        )
    )

    # Timeseries encoding legend (edge)
    legend_handles.append(
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            label="Timeseries available (edge=black)",
            markerfacecolor="white",
            markeredgecolor="black",
            markersize=6,
        )
    )
    legend_handles.append(
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            label="No timeseries (edge=gray)",
            markerfacecolor="white",
            markeredgecolor="#bbbbbb",
            markersize=6,
        )
    )

    ax.legend(
        handles=legend_handles,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=True,
        title="Legend",
    )

    plt.tight_layout()

    out_dir.mkdir(parents=True, exist_ok=True)
    outpath = out_dir / f"{rank:02d}_{year}_map.png"
    plt.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)

    return outpath


def export_tables(all_detail: pd.DataFrame, top_years: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # all_detail.to_parquet(out_dir / f"calibration_candidates.parquet", index=False)
    # all_detail.to_json(out_dir / f"calibration_candidates.json", orient="records", indent=2, date_format="iso")

    top_years.to_csv(out_dir / f"calibration_candidates.csv", index=False)


def add_calibration_events_to_collection(gages_stac: Union[pystac.Catalog, pystac.Collection], tolerance_days: int = 7, top_n_years: int = 15) -> Tuple[pd.DataFrame, pd.DataFrame, List[Path]]:
    """
    Add calibration events to the collection based on gage data analysis.

    Args:
        gages_stac (Union[pystac.Catalog, pystac.Collection]): The gage STAC Catalog or Collection.
        tolerance_days (int): The number of tolerance days for window selection.
        top_n_years (int): The number of top years to process and plot.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, List[Path]]: A tuple containing the top years summary DataFrame, 
        detailed event DataFrame, and a list of paths to the generated plots.
    """    
    conn = duckdb.connect(":memory:")

    if isinstance(gages_stac, pystac.Collection):
        collection = gages_stac
    elif isinstance(gages_stac, pystac.Catalog):
        collection = gages_stac.get_child("gages")
    else:
        raise ValueError("gages_stac must be a pystac Collection or Catalog containing a 'gages' collection.")

    gage_count, gage_availability = load_gages_data(conn, collection)
    total_records = conn.execute(SQL_COUNT_RECORDS).fetchone()[0]
    logging.info(f"Loaded {total_records} AMS records from {gage_count} gages")

    rp_tables = preload_rp_tables(collection)

    # One best window per year (idempotent)
    yearly_summary, year_to_detail = select_best_window_per_year(
        conn=conn,
        rp_tables=rp_tables,
        gage_availability=gage_availability,
        tol_days=tolerance_days,
    )

    # Top 10 years (each plot is a different year)
    top_years = yearly_summary.head(top_n_years).copy()
    logging.info("\nTop years (each with its best window):")
    logging.info(top_years.to_string(index=False))

    base_dir = Path(collection.get_self_href().replace("collection.json", ""))
    output_dir = base_dir / PLOT_DIRNAME

    all_details = []
    plot_paths = []
    map_paths = []


    for rank, (_, row) in enumerate(top_years.iterrows(), start=1):
        year = int(row["year"])
        det = year_to_detail[year].copy()
        det["year"] = year
        det["rank"] = rank

        p = plot_year_event(det, year=year, rank=rank, out_dir=output_dir)
        plot_paths.append(p)

        pm = plot_year_event_map(det, year=year, rank=rank, collection=collection, out_dir=output_dir)
        map_paths.append(pm)
        all_details.append(det)

    all_detail = pd.concat(all_details, ignore_index=True)
    export_tables(all_detail, top_years, output_dir)

    logging.info(f"\nExported to: {output_dir}")

    # Add each file as an asset
    asset_files = plot_paths + map_paths + [output_dir / "calibration_candidates.csv"]
    collection_dir = base_dir

    for file_path in asset_files:
        if not file_path.exists():
            continue

        try:
            rel_path = file_path.relative_to(collection_dir)
        except ValueError:
            rel_path = file_path
        
        filename = file_path.name

        # Determine media type and asset key
        if filename.endswith('.png'):
            media_type = pystac.MediaType.PNG
            # Create asset key from filename
            asset_key = f"calibration_candidate_{filename[:-4]}"
            description = f"Calibration candidate visualization: {filename}"
            role = "visualization"
        elif filename.endswith('.csv'):
            media_type = 'text/csv'
            asset_key = "calibration_candidates_summary"
            description = "Summary of calibration candidates with event statistics"
            role = "data"
        else:
            continue

        # Create and add asset with relative href
        try:
            asset = pystac.Asset(
                href=str(rel_path),
                media_type=media_type,
                description=description,
                roles=[role]
            )
            
            collection.add_asset(asset_key, asset)
            logging.info(f"Added asset '{asset_key}' with relative href: {rel_path}")
        except Exception as e:
            logging.error(f"Failed to add asset {asset_key}: {e}")

    # Save the updated collection
    logging.info(f"Saving updated collection to: {collection.get_self_href()}")
    collection.save_object()
    logging.info("Collection updated successfully")

    return top_years, all_detail, plot_paths