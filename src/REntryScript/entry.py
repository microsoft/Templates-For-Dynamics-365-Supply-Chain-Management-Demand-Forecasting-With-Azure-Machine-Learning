# Python entry script for the v2 parallel job.
# v2 parallel jobs need a Python entry script (init/run); they can't run an R
# entry script like the v1 ParallelRunStep did, so this shim forwards each
# mini-batch to the unchanged forecast.r via Rscript and returns its output lines.
import math
import os
import re
import subprocess
import tempfile

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_FORECAST_R = os.path.join(_THIS_DIR, "forecast.r")
_R_WRAPPER = os.path.join(_THIS_DIR, "forecast_entry.r")

# Whole-number decimal string, e.g. "24.0" / "0.00".
_WHOLE_NUMBER_RE = re.compile(r"-?\d+\.0+")


def _v1_str(value):
    # Mirror the v1 all-string input (no trailing ".0" on whole numbers).
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        if value.is_integer():
            return str(int(value))
        return str(value)
    text = str(value)
    # forecast.r parses some params with strtoi(), which rejects "0.0"; strip a
    # trailing ".0" from whole-number strings so they match v1's integer inputs.
    if _WHOLE_NUMBER_RE.fullmatch(text):
        return text.split(".", 1)[0]
    return text


def init():
    pass


def run(mini_batch):
    # The harness hands a pandas DataFrame. Group by GranularityAttributeKey so
    # each forecast.r call gets ONE key's full history -- forecast.r reads its
    # parameters from the first row, so different keys must not be mixed in a call.
    if mini_batch is None or len(mini_batch) == 0:
        return []

    key = "GranularityAttributeKey"
    if key in mini_batch.columns:
        groups = [g for _, g in mini_batch.groupby(key, sort=False)]
    else:
        groups = [mini_batch]

    results = []
    for group in groups:
        frame = group.copy()
        for column in frame.columns:
            frame[column] = frame[column].map(_v1_str)
        with tempfile.TemporaryDirectory() as tmp:
            in_csv = os.path.join(tmp, "minibatch.csv")
            out_txt = os.path.join(tmp, "result.txt")
            frame.to_csv(in_csv, index=False)
            proc = subprocess.run(
                ["Rscript", _R_WRAPPER, in_csv, out_txt, _FORECAST_R],
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"Rscript failed ({len(frame)} rows): {proc.stderr or proc.stdout}"
                )
            if os.path.exists(out_txt):
                with open(out_txt, encoding="utf-8") as handle:
                    results.extend(ln.rstrip("\r\n") for ln in handle if ln.strip())
    return results
