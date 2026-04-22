#!/usr/bin/env python
"""
Convert Apollo SCB ZIP exports (CSV inside ZIP) into upload-ready Excel files.

This script reads multiple SCB ZIP files (e.g. A/B/C/D parts), merges them,
and writes:
  - NTPC-style workbook for Metadata -> Raw Data auto-ingest flow (SCB parser)
  - Flat SCB workbook (timestamp/inverter_id/scb_id/dc_current) for inspection
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
DEFAULT_DOWNLOADS_DIR = str(Path.home() / "Downloads")

# Quick-edit defaults (you can change these directly).
DEFAULT_SCB_ZIPS = [
    r"C:\Users\ayush.r\Downloads\SCB DATA-A.zip",
    r"C:\Users\ayush.r\Downloads\SCB DATA-B.zip",
    r"C:\Users\ayush.r\Downloads\SCB DATA-C.zip",
    r"C:\Users\ayush.r\Downloads\SCB DATA-D.zip",
]
DEFAULT_OUTPUT_DIR = DEFAULT_DOWNLOADS_DIR

LETTER_TO_INV_NUM = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}


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


def _parse_apollo_scb_header(col: str) -> Optional[Tuple[str, int, str, int]]:
    """
    Example column:
      NTPC NOKHRA | B01-A-CB07 - CB Current (A)
    -> ("B01-A-CB07", 1, "A", 7)
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
    if "cb current" not in raw_metric.strip().lower():
        return None

    m = re.match(r"^B(\d{1,2})-([A-Z])-CB(\d{1,2})$", tag.strip().upper())
    if not m:
        return None
    icr = int(m.group(1))
    inv_letter = m.group(2)
    cb_num = int(m.group(3))
    return tag.strip().upper(), icr, inv_letter, cb_num


def _zip_df_to_long(df: pd.DataFrame) -> pd.DataFrame:
    if APOLLO_TIME_COL not in df.columns:
        raise ValueError(f"Missing '{APOLLO_TIME_COL}' column in Apollo CSV")

    parsed: Dict[str, Tuple[str, int, str, int]] = {}
    for c in df.columns:
        pm = _parse_apollo_scb_header(c)
        if pm:
            parsed[c] = pm
    if not parsed:
        raise ValueError("No recognized Apollo SCB current columns found")

    work = df[[APOLLO_TIME_COL] + list(parsed.keys())].copy()
    work[APOLLO_TIME_COL] = pd.to_datetime(work[APOLLO_TIME_COL], errors="coerce")
    work = work.dropna(subset=[APOLLO_TIME_COL])
    if work.empty:
        return pd.DataFrame(
            columns=["timestamp", "tag", "icr", "inv_letter", "cb_num", "value"]
        )

    melted = work.melt(id_vars=[APOLLO_TIME_COL], var_name="src_col", value_name="value")
    melted[["tag", "icr", "inv_letter", "cb_num"]] = melted["src_col"].apply(
        lambda c: pd.Series(parsed[c])
    )
    melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
    melted = melted.dropna(subset=["value"])
    melted = melted.rename(columns={APOLLO_TIME_COL: "timestamp"})[
        ["timestamp", "tag", "icr", "inv_letter", "cb_num", "value"]
    ]
    return melted


def _merge_long(dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    merged = pd.concat(list(dfs), ignore_index=True)
    if merged.empty:
        return pd.DataFrame(columns=["timestamp", "tag", "icr", "inv_letter", "cb_num", "value"])
    out = (
        merged.groupby(["timestamp", "tag", "icr", "inv_letter", "cb_num"], as_index=False)["value"]
        .mean()
        .sort_values(["timestamp", "icr", "inv_letter", "cb_num"])
        .reset_index(drop=True)
    )
    return out


def _inv_id(icr: int, inv_letter: str) -> str:
    return f"INV-{int(icr):02d}{str(inv_letter).upper()}"


def _scb_id(icr: int, inv_letter: str, cb_num: int) -> str:
    return f"INV-{int(icr):02d}{str(inv_letter).upper()}-SCB-{int(cb_num):02d}"


def _build_flat_template(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    out["inverter_id"] = out.apply(lambda r: _inv_id(r["icr"], r["inv_letter"]), axis=1)
    out["scb_id"] = out.apply(lambda r: _scb_id(r["icr"], r["inv_letter"], r["cb_num"]), axis=1)
    out = out.rename(columns={"value": "dc_current"})
    cols = ["timestamp", "inverter_id", "scb_id", "dc_current"]
    return out[cols].sort_values(["timestamp", "inverter_id", "scb_id"]).reset_index(drop=True)


def _build_ntpc_upload_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build worksheet matching backend NTPC SCB parser:
      row 6 -> ICRxx
      row 7 -> INVn
      row 8 -> DC_INPUT_CURRENTnn
      row 9+ -> timestamped values
    """
    work = df.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
    work = work.dropna(subset=["timestamp"]).sort_values("timestamp")

    col_specs: List[Tuple[int, int, int, str]] = []
    keys = (
        work[["icr", "inv_letter", "cb_num", "tag"]]
        .drop_duplicates()
        .sort_values(["icr", "inv_letter", "cb_num"])
        .itertuples(index=False)
    )
    for icr, inv_letter, cb_num, tag in keys:
        inv_num = LETTER_TO_INV_NUM.get(str(inv_letter).upper())
        if inv_num is None:
            continue
        col_specs.append((int(icr), int(inv_num), int(cb_num), str(tag)))

    if not col_specs:
        raise ValueError("No NTPC-compatible SCB columns could be built from source data.")

    timestamps = sorted(work["timestamp"].dropna().unique())
    n_rows = 9 + len(timestamps)
    n_cols = 1 + len(col_specs)
    matrix: List[List[object]] = [["" for _ in range(n_cols)] for _ in range(n_rows)]

    matrix[2][0] = "REPORT"
    matrix[6][0] = "DATE AND TIME"
    matrix[8][0] = "DATE AND TIME"

    for j, (icr, inv_num, cb_num, _tag) in enumerate(col_specs, start=1):
        matrix[6][j] = f"ICR{icr:02d}"
        matrix[7][j] = f"INV{inv_num}"
        matrix[8][j] = f"DC_INPUT_CURRENT{cb_num:02d}"

    ts_to_row = {ts: 9 + i for i, ts in enumerate(timestamps)}
    for ts in timestamps:
        matrix[ts_to_row[ts]][0] = pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    lookup = work.set_index(["timestamp", "tag"])
    for j, (_icr, _inv_num, _cb_num, tag) in enumerate(col_specs, start=1):
        for ts in timestamps:
            v = None
            try:
                v = lookup.at[(ts, tag), "value"]
            except KeyError:
                v = None
            if pd.notna(v):
                matrix[ts_to_row[ts]][j] = float(v)

    return pd.DataFrame(matrix)


def convert_apollo_scb_zips(
    scb_zip_paths: List[Path],
    out_dir: Path,
    out_prefix: str = "apollo_scb",
) -> Tuple[Path, Path]:
    tracker = ProgressTracker(100.0)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Starting SCB parse...")
    valid_paths = [p for p in scb_zip_paths if p.exists()]
    if not valid_paths:
        raise ValueError("None of the provided SCB ZIP paths exist.")

    per_zip = []
    load_weight = 20.0 / max(len(valid_paths), 1)
    parse_weight = 35.0 / max(len(valid_paths), 1)

    for p in valid_paths:
        df = _read_first_csv_from_zip(p)
        print(f"Loaded {p.name}: {len(df):,} rows, {len(df.columns):,} columns")
        tracker.step(f"Loaded {p.name}", load_weight)

        long_df = _zip_df_to_long(df)
        print(f"Parsed {p.name}: {len(long_df):,} SCB rows")
        tracker.step(f"Parsed {p.name}", parse_weight)
        per_zip.append(long_df)

    merged = _merge_long(per_zip)
    if merged.empty:
        raise ValueError("Merged SCB dataset is empty after parsing ZIP files.")
    print(f"Merged SCB records: {len(merged):,}")
    tracker.step("Merged SCB datasets", 10)

    flat = _build_flat_template(merged)
    tracker.step("Built flat template", 10)
    ntpc = _build_ntpc_upload_matrix(merged)
    tracker.step("Built NTPC upload matrix", 20)

    flat_path = out_dir / f"{out_prefix}_flat_template.xlsx"
    ntpc_path = out_dir / f"{out_prefix}_ntpc_upload.xlsx"

    def _write_with_fallback(df_obj: pd.DataFrame, path: Path, header: bool) -> Path:
        try:
            with pd.ExcelWriter(path, engine="xlsxwriter") as w:
                df_obj.to_excel(w, index=False, header=header, sheet_name="Data" if header else "Report")
            return path
        except PermissionError:
            ts = time.strftime("%Y%m%d_%H%M%S")
            alt = path.with_name(f"{path.stem}_{ts}{path.suffix}")
            with pd.ExcelWriter(alt, engine="xlsxwriter") as w:
                df_obj.to_excel(w, index=False, header=header, sheet_name="Data" if header else "Report")
            print(f"File in use, wrote alternate output: {alt}")
            return alt

    flat_path = _write_with_fallback(flat, flat_path, header=True)
    ntpc_path = _write_with_fallback(ntpc, ntpc_path, header=False)
    tracker.step("Wrote output files", 5)

    return flat_path, ntpc_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Apollo SCB ZIP exports to upload-ready Excel.")
    parser.add_argument(
        "--scb-zips",
        nargs="+",
        default=DEFAULT_SCB_ZIPS,
        help="List of SCB ZIP files (A/B/C/D parts).",
    )
    parser.add_argument(
        "--out-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for generated files.",
    )
    parser.add_argument("--out-prefix", default="apollo_scb", help="Output file prefix.")
    args = parser.parse_args()

    flat_path, ntpc_path = convert_apollo_scb_zips(
        scb_zip_paths=[Path(p) for p in args.scb_zips],
        out_dir=Path(args.out_dir),
        out_prefix=args.out_prefix,
    )
    print(f"Created: {flat_path}")
    print(f"Created: {ntpc_path}")
    print("Upload this file in Website -> Metadata -> Raw Data:")
    print(f"  {ntpc_path}")


if __name__ == "__main__":
    main()
