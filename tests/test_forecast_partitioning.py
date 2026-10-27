import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import mltable
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "REntryScript"))
import api_trigger
import entry
from partition_input import KEY, partition_history


class ForecastPartitionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.frame = pd.DataFrame(
            [
                ["usmf\x1fitem1", "001", "100", "3", "24", "AUTO", "0"],
                ["usmf\x1fitem2", "002", "100", "4", "12", "NONE", "0.5"],
                ["usmf\x1fitem1", "001", "101", "5", "", "", ""],
                ["usmf\x1fitem2", "002", "101", "6", "", "", ""],
                ["usmf\x1fitem1", "001", "102", "7", "", "", ""],
            ],
            columns=[
                "GranularityAttribute", KEY, "DateKey", "TransactionQty",
                "HORIZON", "FORCE_SEASONALITY", "MISSING_VALUE_SUBSTITUTION",
            ],
        )

    def test_source_mltable_and_materialized_partitions_preserve_strings_and_history(self):
        source = self.root / "source.csv"
        self.frame.to_csv(source, index=False)
        # Exercise both the relay's reference-only input and the CLI's rewrite.
        for read_into_memory in (False, True):
            with self.subTest(read_into_memory=read_into_memory):
                with patch("tempfile.gettempdir", return_value=str(self.root)):
                    source_table = api_trigger.register_mltable_asset(
                        None, str(source), read_into_memory=read_into_memory, register=False
                    )
                original = mltable.load(source_table).to_pandas_dataframe()
                output = self.root / str(read_into_memory)
                partition_history(original, str(output))
                metadata = yaml.safe_load((output / "MLTable").read_text())
                steps = metadata["transformations"]
                read = next(step["read_delimited"] for step in steps if "read_delimited" in step)
                extract = next(
                    step["extract_columns_from_partition_format"]
                    for step in steps if "extract_columns_from_partition_format" in step
                )
                self.assertFalse(read["infer_column_types"])
                self.assertEqual(
                    extract["partition_format"],
                    "key={GranularityAttributeKey}/history.csv",
                )
                self.assertFalse(os.path.isabs(metadata["paths"][0]["pattern"]))
                table = mltable.load(str(output))
                materialized = table.to_pandas_dataframe()
                self.assertEqual(len(materialized), len(self.frame))
                self.assertEqual(set(materialized[KEY]), {"001", "002"})
                self.assertEqual(table.get_partition_count(), 2)
                dispatched_keys = []
                for index in range(table.get_partition_count()):
                    partition = table.select_partitions([index]).to_pandas_dataframe()
                    self.assertEqual(partition[KEY].nunique(), 1)
                    key = partition[KEY].iloc[0]
                    dispatched_keys.append(key)
                    self.assertEqual(len(partition), len(self.frame[self.frame[KEY] == key]))
                self.assertEqual(set(dispatched_keys), {"001", "002"})
                for key, expected in self.frame.groupby(KEY, sort=False):
                    actual = materialized[materialized[KEY] == key]
                    pd.testing.assert_frame_equal(
                        actual[self.frame.columns].reset_index(drop=True),
                        expected.reset_index(drop=True),
                        check_dtype=False,
                    )
                    self.assertTrue(all(isinstance(value, str) for value in actual.to_numpy().flat))
                    history = pd.read_csv(
                        output / f"key={key}" / "history.csv",
                        dtype=str, keep_default_na=False,
                    )
                    self.assertNotIn(KEY, history.columns)
                    self.assertEqual(len(history), len(expected))

    def test_invalid_keys_fail_before_writing_any_partition(self):
        for key in ("", None, "../escape", "a/b", "a\\b", "a.b"):
            with self.subTest(key=key):
                frame = self.frame.copy()
                frame.loc[2, KEY] = key
                with self.assertRaisesRegex(ValueError, "path-safe"):
                    partition_history(frame, str(self.root / "invalid"))
                self.assertFalse((self.root / "invalid").exists())
        with self.assertRaisesRegex(ValueError, "nonempty histories"):
            partition_history(self.frame.iloc[:0], str(self.root / "empty"))
        with self.assertRaisesRegex(ValueError, KEY):
            partition_history(self.frame.drop(columns=[KEY]), str(self.root / "missing"))

    def test_entry_passes_each_complete_history_and_parameters_to_r(self):
        received = []
        output = self.root / "dispatch"
        partition_history(self.frame, str(output))
        table = mltable.load(str(output))

        def fake_rscript(arguments, **kwargs):
            history = pd.read_csv(arguments[2], dtype=str, keep_default_na=False)
            received.append(history)
            key = history[KEY].iloc[0]
            Path(arguments[3]).write_text(f"{key},forecast,1\n{key},forecast,2\n")
            return type("Result", (), {"returncode": 0})()

        with patch.object(entry.subprocess, "run", side_effect=fake_rscript):
            batches = [
                entry.run(table.select_partitions([index]).to_pandas_dataframe())
                for index in range(table.get_partition_count())
            ]
        self.assertEqual([len(batch) for batch in batches], [2, 2])
        self.assertEqual(sorted(line for batch in batches for line in batch), [
            "001,forecast,1", "001,forecast,2", "002,forecast,1", "002,forecast,2"
        ])
        received.sort(key=lambda history: history[KEY].iloc[0])
        self.assertEqual([len(history) for history in received], [3, 2])
        self.assertEqual([history["HORIZON"].iloc[0] for history in received], ["24", "12"])
        self.assertEqual([history["DateKey"].tolist() for history in received], [
            ["100", "101", "102"], ["100", "101"]
        ])

    def test_repository_sample_preserves_all_columns_and_rows(self):
        source = mltable.from_delimited_files(
            [{"file": str(ROOT / "sampleInput.csv")}], infer_column_types=False
        ).to_pandas_dataframe()
        output = self.root / "sample"
        partition_history(source, str(output))
        actual = mltable.load(str(output)).to_pandas_dataframe()
        self.assertEqual(len(actual), len(source))
        self.assertEqual(set(actual[KEY]), set(source[KEY]))
        for key, expected in source.groupby(KEY, sort=False):
            pd.testing.assert_frame_equal(
                actual[actual[KEY] == key][source.columns].reset_index(drop=True),
                expected.fillna("").reset_index(drop=True),
                check_dtype=False,
            )

    def test_pipeline_serializes_preparation_dependency_and_key_dispatch(self):
        code = (
            "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/test"
            "/providers/Microsoft.MachineLearningServices/workspaces/test/codes/forecast/versions/1"
        )
        output_path = "azureml://datastores/test/paths/output/parallel_run_step.txt"
        with patch.object(api_trigger, "_REGISTERED_CODE", code), patch(
            "tempfile.gettempdir", return_value=str(self.root)
        ):
            job = api_trigger.build_pipeline_job(str(self.root), output_path)
        job._validate(raise_error=True)
        serialized = self.root / "pipeline.yml"
        job.dump(serialized)
        specification = yaml.safe_load(serialized.read_text())
        self.assertEqual(len(specification["jobs"]), 2)
        prepare_name, prepare = next(
            (name, node) for name, node in specification["jobs"].items()
            if node["type"] == "command"
        )
        forecast = next(
            node for node in specification["jobs"].values() if node["type"] == "parallel"
        )
        self.assertIn("partition_input.py", prepare["component"]["command"])
        self.assertEqual(prepare["outputs"]["partitioned_data"]["type"], "mltable")
        self.assertEqual(forecast["partition_keys"], [KEY])
        self.assertNotEqual(forecast.get("mini_batch_size"), "500mb")
        self.assertEqual(forecast["inputs"]["job_data_path"]["mode"], "direct")
        self.assertIn(
            f"jobs.{prepare_name}.outputs.partitioned_data",
            forecast["inputs"]["job_data_path"]["path"],
        )
        self.assertEqual(forecast["task"]["append_row_to"], "${{outputs.job_output_path}}")
        self.assertEqual(specification["outputs"]["pipeline_output"]["type"], "uri_file")
        self.assertEqual(specification["outputs"]["pipeline_output"]["path"], output_path)


if __name__ == "__main__":
    unittest.main()
