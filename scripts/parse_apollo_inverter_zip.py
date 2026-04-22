#!/usr/bin/env python
"""
Convert Apollo inverter ZIP exports (CSV inside ZIP) into upload-ready Excel files.

This script reads two ZIP files:
  1) DC metrics ZIP   (DC Current / DC Voltage / DC Power)
  2) AC metrics ZIP   (Active Power / Daily Energy)

Outputs:
  - NTPC-style workbook for Metadata -> Raw Data auto-ingest flow.
  - Flat inverter template workbook (timestamp/equipment_id + metrics) for easy inspection.
"""

from __future__ import annotations

import argparse
import re
import time
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


APOLLO_TIME_COL = "DATETIME"

APOLLO_METRIC_MAP = {
    "DC Current (A)": "dc_current",
    "DC Voltage (V)": "dc_voltage",
    "DC Power (kW)": "dc_power",
    "Active Power (kW)": "ac_power",
    "Daily Energy (kWh)": "daily_energy_kwh",
}

NTPC_SIGNAL_NAME = {
    "ac_power": "AC_ACTIVE_POWER_kW",
    "dc_power": "DC_POWER",
    "dc_current": "DC_CURRENT",
    "dc_voltage": "DC_VOLTAGE",
    "daily_energy_kwh": "AC_ACTIVE_ENERGY_kWh",
}

LETTER_TO_INV_NUM = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}
DEFAULT_DOWNLOADS_DIR = str(Path.home() / "Downloads")

# Quick-edit defaults (you can change these three paths directly).
DEFAULT_DC_ZIP = r"C:\Users\ayush.r\Downloads\Inverter Data.zip"
DEFAULT_AC_ZIP = r"C:\Users\ayush.r\Downloads\Inverter Data (1).zip"
DEFAULT_OUTPUT_DIR = DEFAULT_DOWNLOADS_DIR


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
        print(
            f"[{pct:6.2f}%] {label} | elapsed: {elapsed:6.1f}s | ETA: {eta:6.1f}s"
        )


def _read_first_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if not csv_members:
            raise ValueError(f"No CSV found inside ZIP: {zip_path}")
        with zf.open(csv_members[0]) as f:
            return pd.read_csv(f)


def _parse_apollo_header(col: str) -> Optional[Tuple[str, str]]:
    """
    Example column:
      NTPC NOKHRA | B01-A - DC Current (A)
    -> ("B01-A", "dc_current")
    """
    s = str(col or "").strip()
    if not s or s.upper() == APOLLO_TIME_COL:
        return None
    if "|" not in s or " - " not in s:
        return None
    right = s.split("|", 1)[1].strip()
    if " - " not in right:
        return None
    tag, raw_metric = right.split(" - ", 1)
    metric = APOLLO_METRIC_MAP.get(raw_metric.strip())
    if not metric:
        return None
    return tag.strip(), metric


def _zip_df_to_long(df: pd.DataFrame) -> pd.DataFrame:
    if APOLLO_TIME_COL not in df.columns:
        raise ValueError(f"Missing '{APOLLO_TIME_COL}' column in Apollo CSV")

    parsed: Dict[str, Tuple[str, str]] = {}
    for c in df.columns:
        pm = _parse_apollo_header(c)
        if pm:
            parsed[c] = pm
    if not parsed:
        raise ValueError("No recognized Apollo metric columns found")

    work = df[[APOLLO_TIME_COL] + list(parsed.keys())].copy()
    work[APOLLO_TIME_COL] = pd.to_datetime(work[APOLLO_TIME_COL], errors="coerce")
    work = work.dropna(subset=[APOLLO_TIME_COL])
    if work.empty:
        return pd.DataFrame(columns=["timestamp", "tag", "metric", "value"])

    melted = work.melt(id_vars=[APOLLO_TIME_COL], var_name="src_col", value_name="value")
    melted[["tag", "metric"]] = melted["src_col"].apply(lambda c: pd.Series(parsed[c]))
    melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
    melted = melted.dropna(subset=["value"])
    melted = melted.rename(columns={APOLLO_TIME_COL: "timestamp"})[["timestamp", "tag", "metric", "value"]]
    return melted


def _merge_apollo_long(dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    merged = pd.concat(list(dfs), ignore_index=True)
    if merged.empty:
        return pd.DataFrame(columns=["timestamp", "tag"])
    out = (
        merged.groupby(["timestamp", "tag", "metric"], as_index=False)["value"]
        .mean()
        .pivot_table(index=["timestamp", "tag"], columns="metric", values="value", aggfunc="first")
        .reset_index()
    )
    out.columns.name = None
    out = out.sort_values(["timestamp", "tag"]).reset_index(drop=True)
    return out


def _tag_to_icr_inv(tag: str) -> Tuple[Optional[int], Optional[int], str]:
    """
    B01-A -> (1, 1, "INV-01A")
    """
    s = str(tag or "").strip().upper()
    m = re.match(r"^B(\d{1,2})-([A-Z])$", s)
    if not m:
        return None, None, s
    icr = int(m.group(1))
    letter = m.group(2)
    inv_num = LETTER_TO_INV_NUM.get(letter)
    equipment_id = f"INV-{icr:02d}{letter}" if inv_num is not None else f"INV-{icr:02d}"
    return icr, inv_num, equipment_id


def _build_flat_inverter_template(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    meta = out["tag"].apply(_tag_to_icr_inv)
    out["equipment_id"] = meta.apply(lambda x: x[2])

    cols = ["timestamp", "equipment_id", "dc_current", "dc_voltage", "dc_power", "ac_power", "daily_energy_kwh"]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    out = out[cols].sort_values(["timestamp", "equipment_id"]).reset_index(drop=True)
    return out


def _build_ntpc_upload_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a worksheet matching backend metadata NTPC parser expectations:
      row 6 -> ICRxx
      row 7 -> INVn
      row 8 -> signal name
      row 9+ -> timestamped values
    """
    work = df.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
    work = work.dropna(subset=["timestamp"]).sort_values("timestamp")

    metric_order = ["ac_power", "dc_power", "dc_current", "dc_voltage", "daily_energy_kwh"]
    col_specs: List[Tuple[str, int, int, str]] = []

    for tag in sorted(work["tag"].dropna().unique()):
        icr, inv_num, _eq_id = _tag_to_icr_inv(tag)
        if icr is None or inv_num is None:
            continue
        for metric in metric_order:
            if metric not in work.columns:
                continue
            # Keep column only if at least one non-null value exists for this inverter+metric.
            sub = work.loc[work["tag"] == tag, metric]
            if sub.notna().any():
                col_specs.append((metric, icr, inv_num, str(tag)))

    if not col_specs:
        raise ValueError("No NTPC-compatible inverter columns could be built from source data.")

    timestamps = sorted(work["timestamp"].dropna().unique())
    # Row count = 9 header rows + data rows; Col count = 1 timestamp + metric columns.
    n_rows = 9 + len(timestamps)
    n_cols = 1 + len(col_specs)
    matrix: List[List[object]] = [["" for _ in range(n_cols)] for _ in range(n_rows)]

    matrix[2][0] = "REPORT"
    matrix[6][0] = "DATE AND TIME"
    matrix[8][0] = "DATE AND TIME"

    for j, (metric, icr, inv_num, _tag) in enumerate(col_specs, start=1):
        matrix[6][j] = f"ICR{icr:02d}"
        matrix[7][j] = f"INV{inv_num}"
        matrix[8][j] = NTPC_SIGNAL_NAME.get(metric, metric.upper())

    ts_to_row = {ts: 9 + i for i, ts in enumerate(timestamps)}
    for ts in timestamps:
        matrix[ts_to_row[ts]][0] = pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    lookup = work.set_index(["timestamp", "tag"])
    for j, (metric, _icr, _inv_num, tag) in enumerate(col_specs, start=1):
        for ts in timestamps:
            v = None
            try:
                v = lookup.at[(ts, tag), metric]
            except KeyError:
                v = None
            if pd.notna(v):
                matrix[ts_to_row[ts]][j] = float(v)

    return pd.DataFrame(matrix)


def convert_apollo_zip_pair(
    dc_zip_path: Path,
    ac_zip_path: Path,
    out_dir: Path,
    out_prefix: str = "apollo_inverter",
) -> Tuple[Path, Path]:
    tracker = ProgressTracker(100.0)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Starting parse...")
    dc_df = _read_first_csv_from_zip(dc_zip_path)
    print(f"DC ZIP loaded: {len(dc_df):,} rows, {len(dc_df.columns):,} columns")
    tracker.step("Loaded DC ZIP", 10)
    ac_df = _read_first_csv_from_zip(ac_zip_path)
    print(f"AC ZIP loaded: {len(ac_df):,} rows, {len(ac_df.columns):,} columns")
    tracker.step("Loaded AC ZIP", 10)

    long_dc = _zip_df_to_long(dc_df)
    print(f"DC parsed rows: {len(long_dc):,}")
    tracker.step("Parsed DC metrics", 20)
    long_ac = _zip_df_to_long(ac_df)
    print(f"AC parsed rows: {len(long_ac):,}")
    tracker.step("Parsed AC metrics", 20)
    merged = _merge_apollo_long([long_dc, long_ac])
    if merged.empty:
        raise ValueError("Merged dataset is empty after parsing both ZIP files.")
    print(f"Merged records: {len(merged):,}")
    tracker.step("Merged datasets", 10)

    flat = _build_flat_inverter_template(merged)
    tracker.step("Built flat template", 10)
    ntpc = _build_ntpc_upload_matrix(merged)
    tracker.step("Built NTPC upload matrix", 15)

    flat_path = out_dir / f"{out_prefix}_flat_template.xlsx"
    ntpc_path = out_dir / f"{out_prefix}_ntpc_upload.xlsx"

    with pd.ExcelWriter(flat_path, engine="xlsxwriter") as w:
        flat.to_excel(w, index=False, sheet_name="Data")
    with pd.ExcelWriter(ntpc_path, engine="xlsxwriter") as w:
        ntpc.to_excel(w, index=False, header=False, sheet_name="Report")
    tracker.step("Wrote output files", 5)

    return flat_path, ntpc_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Apollo inverter ZIP exports to upload-ready Excel.")
    parser.add_argument(
        "--dc-zip",
        default=DEFAULT_DC_ZIP,
        help="ZIP containing DC Current/Voltage/Power CSV.",
    )
    parser.add_argument(
        "--ac-zip",
        default=DEFAULT_AC_ZIP,
        help="ZIP containing Active Power/Daily Energy CSV.",
    )
    parser.add_argument(
        "--out-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for generated files.",
    )
    parser.add_argument("--out-prefix", default="apollo_inverter", help="Output file prefix.")
    args = parser.parse_args()

    flat_path, ntpc_path = convert_apollo_zip_pair(
        dc_zip_path=Path(args.dc_zip),
        ac_zip_path=Path(args.ac_zip),
        out_dir=Path(args.out_dir),
        out_prefix=args.out_prefix,
    )
    print(f"Created: {flat_path}")
    print(f"Created: {ntpc_path}")
    print("Upload this file in Website -> Metadata -> Raw Data:")
    print(f"  {ntpc_path}")


if __name__ == "__main__":
    main()
