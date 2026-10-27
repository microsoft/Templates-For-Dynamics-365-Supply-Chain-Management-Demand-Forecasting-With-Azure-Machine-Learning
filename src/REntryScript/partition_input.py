"""Materialize key partitions and the MLTable path metadata used by SDK v2."""
import argparse
import os
import re

import mltable

KEY = "GranularityAttributeKey"


def partition_history(frame, output_dir):
    """Keep every key's rows in source order, including its sparse parameters."""
    if KEY not in frame.columns or frame.empty:
        raise ValueError(f"Forecast input must contain nonempty histories with {KEY}.")
    # D365 supplies integer keys as strings. Never normalize unsafe path values:
    # replacing characters could combine different keys or overwrite histories.
    keys = frame[KEY]
    if not keys.map(lambda value: isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]+", value) is not None).all():
        raise ValueError(f"{KEY} must contain nonempty path-safe strings (letters, digits, '_' or '-').")

    for key, history in frame.groupby(KEY, sort=False):
        partition_dir = os.path.join(output_dir, f"key={key}")
        os.makedirs(partition_dir, exist_ok=True)
        history.drop(columns=[KEY]).to_csv(
            os.path.join(partition_dir, "history.csv"), index=False
        )

    # Follow the official partition-by-key example: the dispatch key must be
    # extracted from file paths, not just present as a column in a flat CSV.
    table = mltable.from_delimited_files(
        [{"pattern": os.path.join(output_dir, "key=*", "history.csv").replace("\\", "/")}],
        delimiter=",",
        header="all_files_same_headers",
        encoding="utf8",
        infer_column_types=False,
        empty_as_string=True,
        include_path_column=True,
    )
    table = table.extract_columns_from_partition_format(f"key={{{KEY}}}/history.csv")
    table = table.drop_columns(["Path"])
    table.save(output_dir)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_data", required=True)
    parser.add_argument("--output_data", required=True)
    args = parser.parse_args()
    frame = mltable.load(args.input_data).to_pandas_dataframe()
    partition_history(frame, args.output_data)


if __name__ == "__main__":
    main()
