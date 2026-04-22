#!/usr/bin/env python
"""
Convert Apollo WMS ZIP export (CSV inside ZIP) into upload-ready Excel files.

Outputs:
  - NTPC-style workbook for Metadata -> Raw Data auto-ingest flow.
  - Flat WMS workbook (timestamp + mapped weather signals) for inspection.
"""

from __future__ import annotations

import argparse
import time
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


APOLLO_TIME_COL = "DATETIME"
DEFAULT_DOWNLOADS_DIR = str(Path.home() / "Downloads")

# Quick-edit defaults.
DEFAULT_WMS_ZIP = r"C:\Users\ayush.r\Downloads\WMS.zip"
DEFAULT_OUTPUT_DIR = DEFAULT_DOWNLOADS_DIR

# Apollo metric -> (NTPC display signal, flat signal, multiplier)
WMS_METRIC_MAP: Dict[str, Tuple[str, str, float]] = {
    "Live GHI Power (kW/m^2)": ("GHI Main (W/m2)", "ghi", 1000.0),
    "Live POA Power (kW/m^2)": ("GTI Main (W/m2)", "gti", 1000.0),
    "Ambient Temperature (DEGREE CELSIUS)": ("Ambient Temperature (Deg C)", "ambient_temp", 1.0),
    "Module Temperature (DEGREE CELSIUS)": ("Module Temperature (Deg C)", "module_temp", 1.0),
    "Wind Speed (m/s)": ("Wind Speed (m/s)", "wind_speed", 1.0),
}


class ProgressTracker:
    def __init__(self, total_weight: float = 100.0) -> None:
        self.total_weight = float(total_weight)
        self.done_weight = 0.0
        self.start_ts = time.time()

    def step(self, label: str, weight: float) -> None:
        self.done_weight += float(weight)
        self.done_weight = min(self.done_weight, self.total_weight)
        elapsed = max(time.time() - self.start_ts, 0.001)
        pct = (self.done_weight / self.total_weight) * 100.0
        rate = self.done_weight / elapsed
        rem_weight = max(self.total_weight - self.done_weight, 0.0)
        eta = rem_weight / rate if rate > 0 else 0.0
        print(f"[{pct:6.2f}%] {label} | elapsed: {elapsed:6.1f}s | ETA: {eta:6.1f}s")


def _read_first_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if not csv_members:
            raise ValueError(f"No CSV found inside ZIP: {zip_path}")
        with zf.open(csv_members[0]) as f:
            return pd.read_csv(f)


def _parse_wms_header(col: str) -> Optional[Tuple[str, str, float]]:
    """
    Example:
      NTPC NOKHRA | WMS - Live GHI Power (kW/m^2)
    -> ("GHI Main (W/m2)", "ghi", 1000.0)
    """
    s = str(col or "").strip()
    if not s or s.upper() == APOLLO_TIME_COL:
        return None
    if "|" not in s or " - " not in s:
        return None
    right = s.split("|", 1)[1].strip()
    if " - " not in right:
        return None
    _tag, raw_metric = right.split(" - ", 1)
    return WMS_METRIC_MAP.get(raw_metric.strip())


def _zip_df_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    if APOLLO_TIME_COL not in df.columns:
        raise ValueError(f"Missing '{APOLLO_TIME_COL}' column in Apollo CSV")

    parsed: Dict[str, Tuple[str, str, float]] = {}
    for c in df.columns:
        pm = _parse_wms_header(c)
        if pm:
            parsed[c] = pm
    if not parsed:
        raise ValueError("No recognized Apollo WMS columns found")

    keep_cols = [APOLLO_TIME_COL] + list(parsed.keys())
    work = df[keep_cols].copy()
    work[APOLLO_TIME_COL] = pd.to_datetime(work[APOLLO_TIME_COL], errors="coerce")
    work = work.dropna(subset=[APOLLO_TIME_COL])
    if work.empty:
        return pd.DataFrame(columns=["timestamp"])

    out = pd.DataFrame({"timestamp": work[APOLLO_TIME_COL]})
    for src_col, (ntpc_name, flat_name, mult) in parsed.items():
        vals = pd.to_numeric(work[src_col], errors="coerce") * mult
        out[flat_name] = vals
        out[f"__ntpc__{flat_name}"] = ntpc_name

    # Consolidate duplicate timestamps by mean.
    value_cols = [c for c in out.columns if not c.startswith("__ntpc__")]
    out = out.groupby("timestamp", as_index=False)[value_cols[1:]].mean(numeric_only=True).merge(
        out[["timestamp"]].drop_duplicates(), on="timestamp", how="right"
    )
    out = out.sort_values("timestamp").reset_index(drop=True)

    # Reattach mapping columns as constants for downstream matrix build.
    for _src, (_ntpc, flat_name, _m) in parsed.items():
        out[f"__ntpc__{flat_name}"] = _ntpc
    return out


def _build_flat_template(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    cols = ["timestamp", "ghi", "gti", "ambient_temp", "module_temp", "wind_speed"]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols].sort_values("timestamp").reset_index(drop=True)


def _build_ntpc_upload_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build NTPC-like matrix recognized by analyzer/parser.
    We place synthetic ICR/INV headers so analyzer marks it as NTPC format.
    """
    work = df.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
    work = work.dropna(subset=["timestamp"]).sort_values("timestamp")

    signal_cols = [c for c in ["ghi", "gti", "ambient_temp", "module_temp", "wind_speed"] if c in work.columns]
    signal_cols = [c for c in signal_cols if work[c].notna().any()]
    if not signal_cols:
        raise ValueError("No NTPC-compatible WMS signals could be built from source data.")

    ntpc_names = {}
    for c in signal_cols:
        n = None
        key = f"__ntpc__{c}"
        if key in work.columns and work[key].notna().any():
            n = str(work[key].dropna().iloc[0]).strip()
        if not n:
            # fallback names that backend understands via _map_wms_signals
            if c == "ghi":
                n = "GHI Main (W/m2)"
            elif c == "gti":
                n = "GTI Main (W/m2)"
            elif c == "ambient_temp":
                n = "Ambient Temperature (Deg C)"
            elif c == "module_temp":
                n = "Module Temperature (Deg C)"
            elif c == "wind_speed":
                n = "Wind Speed (m/s)"
            else:
                n = c
        ntpc_names[c] = n

    timestamps = sorted(work["timestamp"].dropna().unique())
    n_rows = 9 + len(timestamps)
    n_cols = 1 + len(signal_cols)
    matrix: List[List[object]] = [["" for _ in range(n_cols)] for _ in range(n_rows)]

    matrix[2][0] = "REPORT"
    matrix[6][0] = "DATE AND TIME"
    matrix[8][0] = "DATE AND TIME"

    # Synthetic ICR/INV labels required by /upload-raw-data-analyze NTPC detector.
    for j, col in enumerate(signal_cols, start=1):
        matrix[6][j] = "ICR00"
        matrix[7][j] = "INV0"
        matrix[8][j] = ntpc_names[col]

    ts_to_row = {ts: 9 + i for i, ts in enumerate(timestamps)}
    for ts in timestamps:
        matrix[ts_to_row[ts]][0] = pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    lookup = work.set_index("timestamp")
    for j, col in enumerate(signal_cols, start=1):
        for ts in timestamps:
            v = None
            try:
                v = lookup.at[ts, col]
            except KeyError:
                v = None
            if pd.notna(v):
                matrix[ts_to_row[ts]][j] = float(v)

    return pd.DataFrame(matrix)


def convert_apollo_wms_zip(
    wms_zip_path: Path,
    out_dir: Path,
    out_prefix: str = "apollo_wms",
) -> Tuple[Path, Path]:
    tracker = ProgressTracker(100.0)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Starting WMS parse...")
    df = _read_first_csv_from_zip(wms_zip_path)
    print(f"WMS ZIP loaded: {len(df):,} rows, {len(df.columns):,} columns")
    tracker.step("Loaded WMS ZIP", 20)

    merged = _zip_df_to_wide(df)
    if merged.empty:
        raise ValueError("WMS dataset is empty after parsing ZIP.")
    print(f"WMS parsed records: {len(merged):,}")
    tracker.step("Parsed WMS metrics", 35)

    flat = _build_flat_template(merged)
    tracker.step("Built flat template", 15)
    ntpc = _build_ntpc_upload_matrix(merged)
    tracker.step("Built NTPC upload matrix", 25)

    flat_path = out_dir / f"{out_prefix}_flat_template.xlsx"
    ntpc_path = out_dir / f"{out_prefix}_ntpc_upload.xlsx"

    with pd.ExcelWriter(flat_path, engine="xlsxwriter") as w:
        flat.to_excel(w, index=False, sheet_name="Data")
    with pd.ExcelWriter(ntpc_path, engine="xlsxwriter") as w:
        ntpc.to_excel(w, index=False, header=False, sheet_name="Report")
    tracker.step("Wrote output files", 5)

    return flat_path, ntpc_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Apollo WMS ZIP export to upload-ready Excel.")
    parser.add_argument("--wms-zip", default=DEFAULT_WMS_ZIP, help="WMS ZIP containing Apollo CSV.")
    parser.add_argument("--out-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory for generated files.")
    parser.add_argument("--out-prefix", default="apollo_wms", help="Output file prefix.")
    args = parser.parse_args()

    flat_path, ntpc_path = convert_apollo_wms_zip(
        wms_zip_path=Path(args.wms_zip),
        out_dir=Path(args.out_dir),
        out_prefix=args.out_prefix,
    )
    print(f"Created: {flat_path}")
    print(f"Created: {ntpc_path}")
    print("Upload this file in Website -> Metadata -> Raw Data:")
    print(f"  {ntpc_path}")


if __name__ == "__main__":
    main()

