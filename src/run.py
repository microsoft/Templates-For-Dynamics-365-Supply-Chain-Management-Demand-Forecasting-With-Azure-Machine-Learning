"""Builds the demand-forecast parallel job (Azure ML SDK v2).

v2 parallel jobs only support a Python entry script (init/run), not an R one like
the v1 ParallelRunStep. entry.py is a small Python shim that forwards each
mini-batch to the unchanged forecast.r via Rscript.
"""
import os
import tempfile

from azure.ai.ml import Input, Output
from azure.ai.ml.constants import AssetTypes, InputOutputModes
from azure.ai.ml.entities import Environment, BuildContext
from azure.ai.ml.parallel import parallel_run_function, RunFunction

import parameters

# Directory holding this file, so paths resolve regardless of the working dir.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_R_SOURCE_DIR = os.path.join(_THIS_DIR, "REntryScript")


def r_environment() -> Environment:
    """Return the R runtime environment (custom Docker image: R + forecast/plyr/zoo)."""
    # Write the build context to a writable temp dir -- the deployed code dir is
    # read-only when this runs inside the relay Function (/home/site/wwwroot).
    build_dir = os.path.join(tempfile.gettempdir(), "demand_forecast_r_env")
    os.makedirs(build_dir, exist_ok=True)

    # Always (re)write so Dockerfile changes (e.g. added pip packages) take effect;
    # a stale file would otherwise reuse the old cached image.
    dockerfile_path = os.path.join(build_dir, "Dockerfile")
    with open(dockerfile_path, "w", encoding="utf-8") as f:
        f.write(_R_DOCKERFILE)

    return Environment(
        name="demand-forecast-r-parallel",
        description="R runtime (forecast, plyr, zoo) for demand forecast parallel job.",
        build=BuildContext(path=os.path.relpath(build_dir, os.getcwd())),
    )


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


def build_parallel_job(input_data, output_data=None, code_asset=None):
    """Create the demand-forecast parallel job (v2).

    code_asset: a pre-registered code-asset id for the R folder. Some SDK versions
    reject an inline local path for task.code, so api_trigger.py registers it and
    passes the id here; falls back to a local path when None.
    """
    if code_asset is not None:
        code_value = code_asset
    else:
        # Local path fallback (newer SDKs / offline construction).
        code_value = "." + os.sep + os.path.relpath(_R_SOURCE_DIR, os.getcwd())

    parallel_job = parallel_run_function(
        name="r_forecast",
        display_name="Generate demand forecast",
        description="Generate demand forecast.",
        inputs=dict(
            job_data_path=Input(
                type=AssetTypes.MLTABLE,
                description="Partitioned historical data to forecast in parallel.",
            )
        ),
        outputs=dict(
            # uri_file: single concatenated output file (matches v1 append_row).
            job_output_path=Output(type=AssetTypes.URI_FILE)
        ),
        input_data="${{inputs.job_data_path}}",
        # Feed the whole (small) tabular input as a single mini-batch; entry.py
        # groups it by GranularityAttributeKey and forecasts each key. (partition_keys
        # did not group reliably here -- the harness produced 1 default partition
        # "000000000" with 0 rows per task.)
        mini_batch_size="500mb",
        instance_count=parameters.nodes_count,
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
