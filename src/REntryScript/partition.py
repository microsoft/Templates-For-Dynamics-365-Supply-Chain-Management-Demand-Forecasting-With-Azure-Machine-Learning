"""Partition the D365 input CSV into series-preserving buckets for the parallel step.

An MLTable can only expose partition keys from its folder layout
(extract_columns_from_partition_format), so this step writes the rows to
<output>/<PartitionBucket>/data.csv and emits that MLTable. The parallel step uses
partition_keys=["PartitionBucket"]: one mini-batch per bucket, and a series
(GranularityAttributeKey) is never split across buckets -- like v1's
partition_by + partition_keys.

Each mini-batch's output is appended to parallel_run_step.txt in one storage
append (max 4 MiB), so buckets are sized from HORIZON to stay well below that.
"""
import argparse
import os

import pandas as pd

KEY_COLUMN = "GranularityAttributeKey"
HORIZON_COLUMN = "HORIZON"
BUCKET_COLUMN = "PartitionBucket"

# Generous upper bound for one forecast.r output line:
# "<attribute>",<datekey>,<qty>,<sigma>,<error%>,<model>
_OUTPUT_LINE_BYTES = 200

_MLTABLE = """type: mltable
paths:
- pattern: ./**/*.csv
transformations:
- read_delimited:
    delimiter: ','
    encoding: utf8
    header: all_files_same_headers
    infer_column_types: false
    include_path_column: true
- extract_columns_from_partition_format:
    partition_format: '{%s}/{file_name}.{extension}'
- drop_columns:
  - Path
  - file_name
  - extension
""" % BUCKET_COLUMN


def series_per_bucket(df, max_series_per_partition, max_partition_output_bytes):
    """Series per bucket so a bucket's output (series x HORIZON lines) fits the byte bound."""
    horizons = pd.to_numeric(df.get(HORIZON_COLUMN, pd.Series(dtype=str)), errors="coerce")
    horizon = max(1, int(horizons.max())) if horizons.notna().any() else 1
    return max(1, min(max_series_per_partition, max_partition_output_bytes // (horizon * _OUTPUT_LINE_BYTES)))


def partition(input_path, output_path, max_series_per_partition, max_partition_output_bytes):
    # Read every value as text, exactly as D365 wrote it (v1 read all columns as
    # strings too); empty cells stay empty so forecast.r sees the same input.
    df = pd.read_csv(input_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    if KEY_COLUMN not in df.columns:
        raise ValueError(f"Input is missing the required column '{KEY_COLUMN}'.")
    if BUCKET_COLUMN in df.columns:
        raise ValueError(f"Input must not contain the reserved column '{BUCKET_COLUMN}'.")
    if len(df) == 0:
        raise ValueError("Input contains no rows to forecast.")

    per_bucket = series_per_bucket(df, max_series_per_partition, max_partition_output_bytes)
    # Series index in order of first appearance; consecutive series share a bucket.
    series_index, keys = pd.factorize(df[KEY_COLUMN], sort=False)
    buckets = series_index // per_bucket
    bucket_count = int(buckets.max()) + 1
    width = len(str(bucket_count - 1))

    os.makedirs(output_path, exist_ok=True)
    # groupby keeps the original row order inside each bucket, so every series keeps
    # its parameter row first (forecast.r reads parameters from the first row).
    for bucket, rows in df.groupby(buckets, sort=True):
        folder = os.path.join(output_path, str(bucket).zfill(width))
        os.makedirs(folder, exist_ok=True)
        rows.to_csv(os.path.join(folder, "data.csv"), index=False, encoding="utf-8")

    with open(os.path.join(output_path, "MLTable"), "w", encoding="utf-8") as handle:
        handle.write(_MLTABLE)

    print(
        f"Partitioned {len(df)} rows / {len(keys)} series into {bucket_count} buckets "
        f"of up to {per_bucket} series."
    )
    return bucket_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_data", required=True, help="Path to the input CSV.")
    parser.add_argument("--output_data", required=True, help="Output folder for the partitioned MLTable.")
    parser.add_argument("--max_series_per_partition", type=int, default=100)
    parser.add_argument("--max_partition_output_bytes", type=int, default=1024 * 1024)
    args = parser.parse_args()
    partition(args.input_data, args.output_data, args.max_series_per_partition, args.max_partition_output_bytes)


if __name__ == "__main__":
    main()
