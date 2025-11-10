import os
import sys
import gzip
import argparse
import pandas as pd
from math import ceil

def parse_args():
    p = argparse.ArgumentParser(description="Split full CSV into daily gzipped CSV partitions.")
    p.add_argument("input_csv", help="Path to input CSV file")
    p.add_argument("out_dir", help="Directory where partition files will be written")
    p.add_argument("target", nargs="?", type=int, default=100000, help="Target rows per output file (default 100000)")
    return p.parse_args()

def main():
    args = parse_args()
    input_csv = args.input_csv
    out_dir = args.out_dir
    target = args.target

    if not os.path.isfile(input_csv):
        print(f"[ERROR] Input file not found: {input_csv}")
        sys.exit(1)

    os.makedirs(out_dir, exist_ok=True)
    print(f"[INFO] Reading {input_csv} ...")
    df = pd.read_csv(input_csv, low_memory=False)
    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Find the date column
    date_cols = [c for c in df.columns if c.lower().strip() in ("order_date", "date")]
    if not date_cols:
        print("[ERROR] No Order_Date column found. Column names detected:")
        print(df.columns.tolist())
        sys.exit(1)
    date_col = date_cols[0]
    print(f"[INFO] Using date column: {date_col}")

    # Convert to date (drop time)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    missing_dates = df[date_col].isna().sum()
    if missing_dates > 0:
        print(f"[WARN] {missing_dates} rows have invalid/missing {date_col} and will be dropped.")
        df = df[df[date_col].notna()]

    total_written = 0
    for d, group in df.groupby(date_col):
        arr = group.copy()
        rows = arr.shape[0]
        if rows == 0:
            continue
        if rows <= target:
            fname = f"orders_date={d}_part0.csv.gz"
            path = os.path.join(out_dir, fname)
            with gzip.open(path, "wt", newline="") as f:
                arr.to_csv(f, index=False)
            print(f"[WRITE] {path} ({rows} rows)")
            total_written += 1
        else:
            parts = ceil(rows / target)
            for i in range(parts):
                chunk = arr.iloc[i * target : (i + 1) * target]
                fname = f"orders_date={d}_part{i}.csv.gz"
                path = os.path.join(out_dir, fname)
                with gzip.open(path, "wt", newline="") as f:
                    chunk.to_csv(f, index=False)
                print(f"[WRITE] {path} ({chunk.shape[0]} rows)")
                total_written += 1

    print(f"[DONE] Wrote {total_written} partition file(s) to: {os.path.abspath(out_dir)}")

if __name__ == "__main__":
    main()
