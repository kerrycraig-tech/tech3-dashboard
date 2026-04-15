#!/usr/bin/env python3
"""
fetch_data.py
Pulls Tech-3 program data from Smartsheet and writes data.json
for the GitHub Pages dashboard.

Usage:
  python scripts/fetch_data.py

Requires:
  SMARTSHEET_TOKEN env var (set as a GitHub Actions secret)
  pip install smartsheet-python-sdk
"""

import json
import os
import sys
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
SHEET_ID = 78430965026692
OUTPUT_PATH = "data.json"
SHEET_URL = "https://app.smartsheet.com/sheets/m3jgRw82F4gxcCg32rxq8fW3FWQQqqmHfRM4JPH1"

# Column IDs (do not change unless the sheet is rebuilt)
COL = {
    "program":       8525617260957572,
    "meeting_date":  362842936348548,
    "status":        4866442563719044,
    "prev_status":   2614642750033796,
    "trend":         7118242377404292,
    "progress":      1488742843191172,
    "blocked":       5992342470561668,
    "sos":           3740542656876420,
    "lead_org":      8244142284246916,
    "owners":        925792889769860,
    "commentary":    5429392517140356,
}

# Canonical program metadata (IDs, names, orgs, owners)
PROGRAMS = [
    {"id": "03.01", "name": "PII Minimization",                  "lead_org": "CyberCRAFT", "owners": "Ashwin / Bernard"},
    {"id": "03.02", "name": "ADU Controls Adoption",             "lead_org": "CyberCRAFT", "owners": "Tapan / Chandra"},
    {"id": "03.03", "name": "FGAC Adoption",                     "lead_org": "CyberCRAFT", "owners": "Tapan / Chandra"},
    {"id": "03.04", "name": "Data Security Controls",            "lead_org": "CyberCRAFT", "owners": "Ashwin / Shankar"},
    {"id": "03.05", "name": "Consent Option Adoption",           "lead_org": "GTM Tech",   "owners": "Ben / Satya"},
    {"id": "03.06", "name": "Removal of Dormant Accounts/Data",  "lead_org": "CyberCRAFT", "owners": "Derek"},
    {"id": "03.07", "name": "Static Code Analyzer",              "lead_org": "FLP Tech",   "owners": "Sandeep"},
    {"id": "03.08", "name": "Standardize/Streamline DSR",        "lead_org": "FLP Tech",   "owners": "Sandeep"},
    {"id": "03.09", "name": "DG4I / Inventory System",           "lead_org": "CyberCRAFT", "owners": "Chandra / Emily"},
]

# Map short codes and full names → canonical ID
PROGRAM_ID_MAP = {}
for p in PROGRAMS:
    pid = p["id"]
    PROGRAM_ID_MAP[pid] = pid                             # "03.01"
    PROGRAM_ID_MAP[f"TECH-{pid}"] = pid                  # "TECH-03.01"
    PROGRAM_ID_MAP[f"TECH-{pid} | {p['name']}"] = pid    # full early format
    PROGRAM_ID_MAP[p["name"].upper()] = pid               # name fallback


def resolve_program_id(raw: str) -> str | None:
    """Map raw Smartsheet program cell value → canonical ID like '03.01'."""
    if not raw:
        return None
    raw = raw.strip()
    if raw in PROGRAM_ID_MAP:
        return PROGRAM_ID_MAP[raw]
    # Try prefix match for partial early-format strings
    for key, val in PROGRAM_ID_MAP.items():
        if raw.startswith(key) or key.startswith(raw):
            return val
    return None


def cell_value(row_cells: dict, col_id: int):
    """Extract display value from a cell dict keyed by column ID."""
    cell = row_cells.get(col_id)
    if cell is None:
        return None
    # Checkboxes return bool in 'value'; text fields use 'displayValue' or 'value'
    return cell.get("displayValue") or cell.get("value")


def fetch_sheet(token: str) -> list[dict]:
    """Fetch all rows from the Smartsheet sheet and return raw row list."""
    try:
        import smartsheet
    except ImportError:
        print("ERROR: smartsheet SDK not installed. Run: pip install smartsheet-python-sdk")
        sys.exit(1)

    client = smartsheet.Smartsheet(token)
    client.errors_as_exceptions(True)
    sheet = client.Sheets.get_sheet(SHEET_ID, include="objectValue")

    # Build index: column_id → title
    col_index = {col.id: col.id for col in sheet.columns}

    rows = []
    for row in sheet.rows:
        cells = {cell.column_id: {"value": cell.value, "displayValue": cell.display_value}
                 for cell in row.cells}
        rows.append(cells)
    return rows


def build_data(rows: list[dict]) -> dict:
    """Transform raw sheet rows into the data.json structure."""
    history = []
    meeting_dates = set()

    for cells in rows:
        raw_program = cell_value(cells, COL["program"])
        raw_date    = cell_value(cells, COL["meeting_date"])
        if not raw_program or not raw_date:
            continue

        prog_id = resolve_program_id(str(raw_program))
        if not prog_id:
            print(f"  WARN: unrecognised program '{raw_program}' — skipped")
            continue

        # Normalise date to YYYY-MM-DD
        try:
            dt = datetime.fromisoformat(str(raw_date).split("T")[0])
            date_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            print(f"  WARN: bad date '{raw_date}' for {prog_id} — skipped")
            continue

        meeting_dates.add(date_str)

        status       = str(cell_value(cells, COL["status"])    or "").strip() or "Grey"
        prev_status  = str(cell_value(cells, COL["prev_status"]) or "").strip() or "N/A"
        trend        = str(cell_value(cells, COL["trend"])      or "").strip() or "Flat"
        progress_raw = cell_value(cells, COL["progress"])
        blocked_raw  = cell_value(cells, COL["blocked"])
        sos_raw      = cell_value(cells, COL["sos"])

        try:
            progress = float(str(progress_raw).replace("%", "")) if progress_raw is not None else 0
        except ValueError:
            progress = 0

        blocked = bool(blocked_raw) if blocked_raw is not None else False
        sos     = bool(sos_raw)     if sos_raw     is not None else False

        history.append({
            "program_id":  prog_id,
            "date":        date_str,
            "status":      status,
            "prev_status": prev_status,
            "trend":       trend,
            "progress":    progress,
            "blocked":     blocked,
            "sos":         sos,
        })

    # Sort history chronologically
    history.sort(key=lambda h: (h["date"], h["program_id"]))
    meetings = sorted(meeting_dates)

    return {
        "meta": {
            "last_updated":    datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "sheet_id":        str(SHEET_ID),
            "sheet_url":       SHEET_URL,
            "total_meetings":  len(meetings),
            "programs_count":  len(PROGRAMS),
        },
        "programs": PROGRAMS,
        "meetings": meetings,
        "history":  history,
    }


def main():
    token = os.environ.get("SMARTSHEET_TOKEN")
    if not token:
        print("ERROR: SMARTSHEET_TOKEN environment variable is not set.")
        sys.exit(1)

    print(f"Fetching sheet {SHEET_ID} from Smartsheet…")
    rows = fetch_sheet(token)
    print(f"  Got {len(rows)} rows")

    data = build_data(rows)
    print(f"  Processed {len(data['history'])} history records across {len(data['meetings'])} meetings")

    # Write to output
    out_path = os.path.join(os.path.dirname(__file__), "..", OUTPUT_PATH)
    out_path = os.path.normpath(out_path)
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Wrote {out_path}")
    print("Done.")


if __name__ == "__main__":
    main()
