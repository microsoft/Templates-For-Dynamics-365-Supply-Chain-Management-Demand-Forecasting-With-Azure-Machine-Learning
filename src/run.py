"""Builds the demand-forecast pipeline steps (Azure ML SDK v2).

v2 parallel jobs only support a Python entry script (init/run), not an R one like
the v1 ParallelRunStep. entry.py is a small Python shim that forwards each
mini-batch to the unchanged forecast.r via Rscript.

partition.py (a command step) first splits the input into series-preserving
buckets, mirroring v1's partition_by + partition_keys, so the parallel step gets
one bounded mini-batch per bucket.
"""
import os
import tempfile

from azure.ai.ml import Input, Output, command
from azure.ai.ml.constants import AssetTypes, InputOutputModes
from azure.ai.ml.entities import Environment, BuildContext
from azure.ai.ml.parallel import parallel_run_function, RunFunction

import parameters

# Directory holding this file, so paths resolve regardless of the working dir.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_R_SOURCE_DIR = os.path.join(_THIS_DIR, "REntryScript")

# Column written by partition.py; each distinct value becomes one mini-batch.
PARTITION_KEY = "PartitionBucket"

_ENVIRONMENT = None


def r_environment() -> Environment:
    """Return the R runtime environment (custom Docker image: R + forecast/plyr/zoo)."""
    # Built once per process so both pipeline steps share one environment object.
    global _ENVIRONMENT
    if _ENVIRONMENT is not None:
        return _ENVIRONMENT

    # Write the build context to a writable temp dir -- the deployed code dir is
    # read-only when this runs inside the relay Function (/home/site/wwwroot).
    build_dir = os.path.join(tempfile.gettempdir(), "demand_forecast_r_env")
    os.makedirs(build_dir, exist_ok=True)

    # Always (re)write so Dockerfile changes (e.g. added pip packages) take effect;
    # a stale file would otherwise reuse the old cached image.
    dockerfile_path = os.path.join(build_dir, "Dockerfile")
    with open(dockerfile_path, "w", encoding="utf-8") as f:
        f.write(_R_DOCKERFILE)

    _ENVIRONMENT = Environment(
        name="demand-forecast-r-parallel",
        description="R runtime (forecast, plyr, zoo) for demand forecast parallel job.",
        build=BuildContext(path=os.path.relpath(build_dir, os.getcwd())),
    )
    return _ENVIRONMENT


# Same R packages as the v1 template, on a current AzureML CPU base image.
_R_DOCKERFILE = r"""
FROM mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest

# Pin pip to avoid a known ruamel installation issue.
RUN conda install -c r -y pip=24.0

# Harness deps: mltable (loads the partitioned input) + azureml-dataprep[pandas]
# (mltable.to_pandas_dataframe needs pandas). Without these the worker fails with
# "ModuleNotFoundError: No module named 'mltable'" / PandasImportError.
RUN pip install azureml-dataset-runtime mltable "azureml-dataprep[pandas]"

# Setup R runtime. Ref: http://cran.rstudio.com/bin/linux/ubuntu/
RUN apt update -qq -y \
  && apt install --no-install-recommends software-properties-common dirmngr -y \
  && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
  && add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/" \
  && add-apt-repository ppa:c2d4u.team/c2d4u4.0+ \
  && apt-get update \
  && apt install r-base r-base-dev littler curl r-cran-curl -y --allow-unauthenticated

# Install packages used by the R agent.
RUN r -e 'install.packages(c("funr", "jsonlite", "logging"))'
RUN r -e 'install.packages("forecast", repos = "http://cran.us.r-project.org")'
RUN r -e 'install.packages("plyr", repos = "http://cran.us.r-project.org")'
RUN r -e 'install.packages("zoo", repos = "http://cran.us.r-project.org")'
"""


def _code_value(code_asset):
    if code_asset is not None:
        return code_asset
    # Local path fallback (newer SDKs / offline construction).
    return "." + os.sep + os.path.relpath(_R_SOURCE_DIR, os.getcwd())


def build_partition_step(input_data, code_asset=None):
    """Create the command step that splits the input CSV into series-preserving
    buckets and emits a partitioned MLTable (see partition.py)."""
    partition_step = command(
        name="partition_input",
        display_name="Partition input by forecast series",
        description="Split the input into buckets that keep each forecast series whole.",
        inputs=dict(input_data=Input(type=AssetTypes.URI_FILE)),
        outputs=dict(partitioned_data=Output(type=AssetTypes.MLTABLE)),
        code=_code_value(code_asset),
        command=(
            "python partition.py"
            " --input_data ${{inputs.input_data}}"
            " --output_data ${{outputs.partitioned_data}}"
            f" --max_series_per_partition {parameters.max_series_per_partition}"
            f" --max_partition_output_bytes {parameters.max_partition_output_bytes}"
        ),
        environment=r_environment(),
    )
    return partition_step(input_data=input_data)


def build_parallel_job(input_data, output_data=None, code_asset=None):
    """Create the demand-forecast parallel job (v2).

    input_data must be the partitioned MLTable produced by build_partition_step.
    code_asset: a pre-registered code-asset id for the R folder. Some SDK versions
    reject an inline local path for task.code, so api_trigger.py registers it and
    passes the id here; falls back to a local path when None.
    """
    code_value = _code_value(code_asset)

    parallel_job = parallel_run_function(
        name="r_forecast",
        display_name="Generate demand forecast",
        description="Generate demand forecast.",
        inputs=dict(
            job_data_path=Input(
                type=AssetTypes.MLTABLE,
                description="Historical data partitioned by PartitionBucket.",
            )
        ),
        outputs=dict(
            # uri_file: single concatenated output file (matches v1 append_row).
            job_output_path=Output(type=AssetTypes.URI_FILE)
        ),
        input_data="${{inputs.job_data_path}}",
        # One mini-batch per bucket; a bucket always holds whole series, so the
        # per-mini-batch output appended to the result file stays small (v1 used
        # partition_keys=['GranularityAttributeKey'] the same way).
        partition_keys=[PARTITION_KEY],
        instance_count=parameters.nodes_count,
        max_concurrency_per_instance=parameters.processes_per_node,
        retry_settings=dict(timeout=parameters.mini_batch_timeout_seconds),
        # Ignore item-level failures (output rows < input rows is expected here).
        error_threshold=-1,
        # Fail the run if any mini-batch fails.
        mini_batch_error_threshold=0,
        logging_level="DEBUG",
        task=RunFunction(
            # Pre-registered code-asset id, or a local path (see docstring).
            code=code_value,
            # v2 needs a Python entry script; entry.py shims to forecast.r via Rscript.
            entry_script="entry.py",
            environment=r_environment(),
            # v1 output_action="append_row" -> v2 append_row_to: aggregate every
            # mini-batch return into one file (headerless, sequential), referencing
            # the job output declared above.
            append_row_to="${{outputs.job_output_path}}",
        ),
    )

    job = parallel_job(job_data_path=input_data)
    # Tabular MLTABLE is consumed in DIRECT mode so run() receives a DataFrame.
    job.inputs.job_data_path.mode = InputOutputModes.DIRECT
    if output_data is not None:
        job.outputs.job_output_path = output_data
    return job
