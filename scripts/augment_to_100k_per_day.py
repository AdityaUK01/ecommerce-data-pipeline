import os
import sys
import argparse
import uuid
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def parse_args():
    p = argparse.ArgumentParser(description="Augment seed CSV to produce N days of ~target rows/day")
    p.add_argument("input_csv")
    p.add_argument("out_dir")
    p.add_argument("start_date", help="YYYY-MM-DD")
    p.add_argument("days", type=int)
    p.add_argument("target", nargs="?", type=int, default=100000)
    return p.parse_args()

def random_time_strings(n):
    seconds = np.random.randint(0, 24*3600, size=n)
    t = pd.to_datetime(seconds, unit='s').time
    return [str(tt) for tt in t]

def main():
    args = parse_args()
    input_csv = args.input_csv
    out_dir = args.out_dir
    start_date = args.start_date
    days = args.days
    target = args.target

    if not os.path.isfile(input_csv):
        print(f"[ERROR] Input file not found: {input_csv}")
        sys.exit(1)
    os.makedirs(out_dir, exist_ok=True)

    print(f"[INFO] Reading seed CSV: {input_csv}")
    df_seed = pd.read_csv(input_csv, low_memory=False)
    df_seed.columns = [c.strip() for c in df_seed.columns]

    # Ensure there's a Customer_Id column; if not, create one
    if "Customer_Id" not in df_seed.columns:
        df_seed["Customer_Id"] = [uuid.uuid4().hex[:12] for _ in range(len(df_seed))]

    # Convert start_date
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    except Exception as e:
        print(f"[ERROR] start_date must be YYYY-MM-DD: {e}")
        sys.exit(1)

    for i in range(days):
        day = start_dt + timedelta(days=i)
        rows_needed = target
        parts = []
        while rows_needed > 0:
            sample_n = min(rows_needed, len(df_seed))
            sample = df_seed.sample(n=sample_n, replace=True).copy()
            # make lightweight modifications to avoid exact duplicates
            sample["Customer_Id"] = sample["Customer_Id"].astype(str) + "_" + [uuid.uuid4().hex[:6] for _ in range(len(sample))]
            # set the Order_Date column (detect name)
            date_cols = [c for c in sample.columns if c.lower().strip() in ("order_date", "date")]
            if date_cols:
                sample[date_cols[0]] = day.strftime("%Y-%m-%d")
            else:
                sample["Order_Date"] = day.strftime("%Y-%m-%d")
            # update Time to a random time if exists
            if "Time" in sample.columns:
                sample["Time"] = random_time_strings(len(sample))
            else:
                sample["Time"] = random_time_strings(len(sample))
            parts.append(sample)
            rows_needed -= len(sample)
        out_df = pd.concat(parts, ignore_index=True)
        out_path = os.path.join(out_dir, f"orders_date={day}.csv.gz")
        out_df.to_csv(out_path, index=False, compression="gzip")
        print(f"[WRITE] {out_path} ({out_df.shape[0]} rows)")

    print(f"[DONE] Generated {days} day(s) of data in: {os.path.abspath(out_dir)}")

if __name__ == "__main__":
    main()
