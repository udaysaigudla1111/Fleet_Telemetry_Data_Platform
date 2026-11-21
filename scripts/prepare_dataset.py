"""Prepare the Kaggle Scania APS CSVs for upload to the raw zone.

SYNTHETIC DATA WARNING
----------------------
The source dataset has no IDs or timestamps. This script ADDS three
synthetic columns so the data looks like fleet telemetry:

    reading_id   e.g. "train-000001"  unique per row (position in source file)
    vehicle_id   e.g. "TRK-0042"      random truck out of a fleet of 2000
    event_date   e.g. "2026-03-14"    random day in the 12 months before --end-date

These values are made up. They are NOT in the original data.

Everything else is copied exactly as-is (including the "na" strings).
Cleaning is the Glue job's job, so raw stays close to the source.

Deterministic: a fixed seed and a fixed end date mean every run produces
byte-identical output, so re-running never silently changes the raw zone.

Usage (from the project root):
    python scripts/prepare_dataset.py
"""

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

# Source file -> split name. The split becomes a folder (Hive-style
# partition "split=train") so Glue/Athena can filter on it later.
SOURCES = {
    "aps_failure_training_set.csv": "train",
    "aps_failure_test_set.csv": "test",
}

FLEET_SIZE = 2000
DAYS_OF_HISTORY = 365
SEED = 42
SYNTHETIC_COLUMNS = ["reading_id", "vehicle_id", "event_date"]


def find_source(input_dir: Path, filename: str) -> Path:
    """Find the file anywhere under input_dir (the Kaggle zip nests it in archive/)."""
    matches = list(input_dir.rglob(filename))
    if len(matches) != 1:
        raise SystemExit(f"Expected exactly one {filename} under {input_dir}, found {len(matches)}")
    return matches[0]


def prepare_split(src: Path, dest: Path, split: str, end_date: date) -> dict:
    # One RNG per split, seeded from the split name: train and test get
    # different but reproducible values, independent of processing order.
    rng = random.Random(f"{SEED}-{split}")
    start_date = end_date - timedelta(days=DAYS_OF_HISTORY - 1)

    dest.parent.mkdir(parents=True, exist_ok=True)
    rows = failures = 0

    # newline="" is what the csv module wants; it stops Windows adding \r\r\n.
    with src.open(newline="") as fin, dest.open("w", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout, lineterminator="\n")

        header = next(reader)
        if header[0] != "class":
            raise SystemExit(f"{src.name}: expected first column 'class', got {header[0]!r}")
        writer.writerow(SYNTHETIC_COLUMNS + header)

        for i, row in enumerate(reader, start=1):
            if len(row) != len(header):
                raise SystemExit(f"{src.name} line {i + 1}: {len(row)} fields, expected {len(header)}")
            reading_id = f"{split}-{i:06d}"
            vehicle_id = f"TRK-{rng.randint(1, FLEET_SIZE):04d}"
            event_date = start_date + timedelta(days=rng.randrange(DAYS_OF_HISTORY))
            writer.writerow([reading_id, vehicle_id, event_date.isoformat()] + row)

            rows += 1
            failures += row[0] == "pos"

    return {"split": split, "rows": rows, "failures": failures, "out": dest,
            "dates": f"{start_date} .. {end_date}"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, default=Path("data/kaggle"))
    parser.add_argument("--output", type=Path, default=Path("data/prepared"))
    # Fixed (not today()) so the output doesn't change depending on when you run it.
    parser.add_argument("--end-date", type=date.fromisoformat, default=date(2026, 10, 31))
    args = parser.parse_args()

    for filename, split in SOURCES.items():
        src = find_source(args.input, filename)
        # Output path mirrors the S3 key we'll upload to:
        #   s3://<raw-bucket>/scania_aps/split=train/aps_failure_training_set.csv
        dest = args.output / "scania_aps" / f"split={split}" / filename
        s = prepare_split(src, dest, split, args.end_date)
        print(f"{s['split']:>5}: {s['rows']:,} rows, {s['failures']:,} failures, "
              f"dates {s['dates']} -> {s['out']}")


if __name__ == "__main__":
    main()
