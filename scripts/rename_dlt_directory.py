#!/usr/bin/env python3
"""Renames databricks/dlt/ to databricks/lakeflow/ and updates all path references."""
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent.parent

SRC_DIR = ROOT / "databricks" / "dlt"
DST_DIR = ROOT / "databricks" / "lakeflow"


def run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"Command {cmd} failed:\n{result.stderr}")
    if result.stdout:
        print(result.stdout.rstrip())


def replace_in_file(path: Path, replacements: list[tuple[str, str]]) -> None:
    original = path.read_text(encoding="utf-8")
    updated = original
    for old, new in replacements:
        updated = updated.replace(old, new)
    if updated != original:
        path.write_text(updated, encoding="utf-8")
        print(f"  Updated: {path.relative_to(ROOT)}")


# ── Step 1: rename directory via git mv ──────────────────────────────────────
print("Step 1: git mv databricks/dlt → databricks/lakeflow")
if not SRC_DIR.exists():
    raise SystemExit(f"Source directory not found: {SRC_DIR}")
if DST_DIR.exists():
    raise SystemExit(f"Destination already exists: {DST_DIR}")

run(["git", "mv", "databricks/dlt", "databricks/lakeflow"])
print(f"  Renamed {SRC_DIR.relative_to(ROOT)} → {DST_DIR.relative_to(ROOT)}")

# ── Step 2: update path references in text files ─────────────────────────────
print("\nStep 2: updating path references in text files")

# pipeline_settings.json: workspace paths use /databricks/dlt/
replace_in_file(
    DST_DIR / "pipeline_settings.json",
    [("/databricks/dlt/", "/databricks/lakeflow/")],
)

# root README.md: links and shell comment reference databricks/dlt/
replace_in_file(
    ROOT / "README.md",
    [("databricks/dlt/", "databricks/lakeflow/")],
)

# IMPROVEMENTS.md: file path references in issue descriptions
replace_in_file(
    ROOT / "IMPROVEMENTS.md",
    [("databricks/dlt/", "databricks/lakeflow/")],
)

# lakeflow README (formerly dlt/README.md):
#   - directory listing root label: "dlt/" at start of a line
#   - any remaining databricks/dlt/ path refs
replace_in_file(
    DST_DIR / "README.md",
    [
        ("databricks/dlt/", "databricks/lakeflow/"),
        ("databricks/dlt\n", "databricks/lakeflow\n"),
        ("\ndlt/\n", "\nlakeflow/\n"),
    ],
)

# presentation.html: code comment shows the file's relative path
replace_in_file(
    ROOT / "presentation.html",
    [("# dlt/silver/", "# lakeflow/silver/")],
)

# migrate script: hardcoded root path (kept for reference, update for accuracy)
replace_in_file(
    ROOT / "scripts" / "migrate_dlt_to_lakeflow.py",
    [('Path("databricks/dlt")', 'Path("databricks/lakeflow")')],
)

# ── Step 3: stage updated files ───────────────────────────────────────────────
print("\nStep 3: staging all changes")
run(["git", "add", "-A"])

# ── Step 4: summary ───────────────────────────────────────────────────────────
print("\nDone. Verify with:")
print("  git diff --cached --stat")
print("  git commit -m 'Rename databricks/dlt/ to databricks/lakeflow/'")
