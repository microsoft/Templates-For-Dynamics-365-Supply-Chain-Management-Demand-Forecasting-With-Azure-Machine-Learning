"""Prepare complete per-key history files and their MLTable partition metadata."""
import argparse
import os
import re

import mltable

KEY = "GranularityAttributeKey"


def partition_history(frame, output_dir):
    """Write one ordered history per key, keeping input fields as strings."""
    if KEY not in frame.columns or frame.empty:
        raise ValueError(f"Forecast input must contain nonempty histories with {KEY}.")

    keys = frame[KEY]
    if not keys.map(
        lambda value: isinstance(value, str)
        and re.fullmatch(r"[A-Za-z0-9_-]+", value) is not None
    ).all():
        raise ValueError(
            f"{KEY} must contain nonempty path-safe strings "
            "(letters, digits, '_' or '-')."
        )

    for key, history in frame.groupby(KEY, sort=False):
        partition_dir = os.path.join(output_dir, f"key={key}")
        os.makedirs(partition_dir, exist_ok=True)
        history.drop(columns=[KEY]).to_csv(
            os.path.join(partition_dir, "history.csv"), index=False
        )

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
    table.drop_columns(["Path"]).save(output_dir)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_data", required=True)
    parser.add_argument("--output_data", required=True)
    args = parser.parse_args()
    frame = mltable.load(args.input_data).to_pandas_dataframe()
    partition_history(frame, args.output_data)


if __name__ == "__main__":
    main()
