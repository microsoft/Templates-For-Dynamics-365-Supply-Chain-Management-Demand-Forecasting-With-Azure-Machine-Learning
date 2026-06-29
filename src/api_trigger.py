"""Trigger / publish the demand-forecast pipeline (Azure ML SDK v2).

v2 replaces Workspace.from_config / Experiment.submit / PipelineEndpoint with
MLClient + an @pipeline DSL job + components.create_or_update. The parallel job is
a first-class pipeline step (no nested-pipeline indirection).
"""
import argparse
import os
from datetime import datetime

from azure.ai.ml import MLClient, Input
from azure.ai.ml.constants import AssetTypes, InputOutputModes
from azure.ai.ml.dsl import pipeline
from azure.identity import DefaultAzureCredential

import parameters
from run import build_parallel_job, _R_SOURCE_DIR

# Holds the registered code-asset reference (set by register_code_asset) so the
# @pipeline-decorated function can read it when building the parallel step.
_REGISTERED_CODE = None


def register_code_asset(ml_client: MLClient):
    """Register the R source folder as a versioned code asset and return its id.

    Some SDK versions (e.g. azure-ai-ml 1.31.0) reject an inline local path for
    task.code and require a registered asset reference, so register it up front.
    """
    from azure.ai.ml.entities._assets import Code

    # 1.31.0 exposes code ops only at the private ._code; newer SDKs add .code.
    code_ops = getattr(ml_client, "code", None) or ml_client._code

    # Server requires a positive-integer version; 1.31.0 CodeOperations has no
    # list(), so find the first unused integer version and create current code there.
    name = "demand-forecast-rentry"
    last_error = None
    for v in range(1, 10001):
        version = str(v)
        # Skip versions that already exist; we want a fresh one for current code.
        try:
            code_ops.get(name=name, version=version)
            continue  # exists -> try next integer
        except Exception:
            pass  # does not exist -> create here
        try:
            code = Code(name=name, version=version, path=_R_SOURCE_DIR)
            registered = code_ops.create_or_update(code)
            # Return the FULL ARM id: on 1.31.0 a short "azureml:name:version" is
            # treated as a local path at submit time and fails; the ARM id is
            # recognized as already-registered, so the path upload is skipped.
            if getattr(registered, "id", None):
                return registered.id
            # Fallback short reference (works on SDKs that resolve it correctly).
            return f"azureml:{registered.name}:{registered.version}"
        except Exception as e:  # rare race: created between get and create -> retry
            last_error = e
            continue
    raise RuntimeError(
        f"Could not register code asset '{name}' after many version attempts; "
        f"last error: {last_error}"
    )


def _expand_datastore_uri(ml_client: MLClient, uri: str) -> str:
    """Expand a short azureml://datastores/<ds>/paths/<p> URI to the long
    subscription/resourcegroup/workspace form. rslex can fail to resolve the short
    form on compute (StreamAccess.NotFound) though the blob exists; the long form
    works. Non-datastore / already-long URIs are returned unchanged.
    """
    prefix = "azureml://datastores/"
    if not uri.startswith(prefix):
        return uri
    rest = uri[len(prefix):]  # "<ds>/paths/<path>"
    return (
        f"azureml://subscriptions/{ml_client.subscription_id}"
        f"/resourcegroups/{ml_client.resource_group_name}"
        f"/workspaces/{ml_client.workspace_name}"
        f"/datastores/{rest}"
    )


def register_mltable_asset(ml_client: MLClient, csv_datastore_uri: str, read_into_memory: bool = True, register: bool = True):
    """Build an MLTABLE around the input CSV; return a registered asset id or, when
    register=False, the local MLTable folder path (used inline as a job input).

    A flat (single-file) MLTable is enough here: the parallel job feeds the whole
    table to run() as one mini-batch and entry.py groups it by GranularityAttributeKey
    at runtime. infer_column_types=False so the parallel harness reads the sparse
    parameter columns as plain strings (otherwise it mis-infers e.g. 0 -> 0.0/False).

    read_into_memory=True reads + rewrites one self-contained data.csv (robust for
    hand-edited inputs). Set it False for large inputs (e.g. from the relay) to make
    the MLTable reference the source blob directly and skip the slow synchronous read.

    register=False returns the local MLTable folder so the caller can pass it as an
    inline job input -- avoids creating a new data-asset version on every forecast.
    """
    import shutil
    import tempfile

    import mltable
    from azure.ai.ml.entities._assets import Data

    # Expand to the long URI form (the short form can fail with NotFound on compute).
    csv_datastore_uri = _expand_datastore_uri(ml_client, csv_datastore_uri)

    mltable_dir = os.path.join(tempfile.gettempdir(), "demand_forecast_mltable")
    if os.path.exists(mltable_dir):
        shutil.rmtree(mltable_dir)
    os.makedirs(mltable_dir, exist_ok=True)

    if read_into_memory:
        # Read the source CSV and write it back as a single local data.csv so the
        # MLTable is fully self-contained, then build the MLTable over that file.
        df = mltable.from_delimited_files(
            [{"file": csv_datastore_uri}],
            delimiter=",",
            header="all_files_same_headers",
            encoding="utf8",
            infer_column_types=False,
        ).to_pandas_dataframe()
        df.to_csv(os.path.join(mltable_dir, "data.csv"), index=False)
        tbl = mltable.from_delimited_files(
            paths=[{"pattern": os.path.join(mltable_dir, "*.csv").replace("\\", "/")}],
            delimiter=",",
            header="all_files_same_headers",
            encoding="utf8",
            infer_column_types=False,
        )
    else:
        # Fast path: reference the source blob directly (no full read at submit time).
        tbl = mltable.from_delimited_files(
            [{"file": csv_datastore_uri}],
            delimiter=",",
            header="all_files_same_headers",
            encoding="utf8",
            infer_column_types=False,
        )
    tbl.save(mltable_dir)

    if not register:
        # Inline input: the job uploads this small MLTable folder per submit (no
        # named data-asset version churn). The MLTable references the source blob.
        return mltable_dir

    name = "demand-forecast-input"
    data_ops = getattr(ml_client, "data", None) or ml_client._data
    last_error = None
    for v in range(1, 10001):
        version = str(v)
        try:
            data_ops.get(name=name, version=version)
            continue  # exists -> next integer
        except Exception:
            pass
        try:
            data_asset = Data(
                name=name,
                version=version,
                type=AssetTypes.MLTABLE,
                path=mltable_dir,
                description="MLTable for the demand forecast input (grouped by key in the entry script).",
            )
            registered = data_ops.create_or_update(data_asset)
            if getattr(registered, "id", None):
                return registered.id
            return f"azureml:{registered.name}:{registered.version}"
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(
        f"Could not register MLTable data asset '{name}'; last error: {last_error}"
    )


def get_ml_client() -> MLClient:
    """Construct an MLClient (parameters.py values, else MLClient.from_config())."""
    credential = DefaultAzureCredential()
    if parameters.subscription_id and parameters.resource_group and parameters.workspace_name:
        return MLClient(
            credential=credential,
            subscription_id=parameters.subscription_id,
            resource_group_name=parameters.resource_group,
            workspace_name=parameters.workspace_name,
        )
    return MLClient.from_config(credential=credential)


@pipeline(
    name="TriggerDemandForecastGeneration",
    description="Initiate demand forecast generation.",
    default_compute=parameters.compute_cluster_name,
)
def demand_forecast_pipeline(input_data: Input):
    """Pipeline wrapping the single R parallel job step."""
    forecast_step = build_parallel_job(
        input_data=input_data, code_asset=_REGISTERED_CODE
    )
    return {"pipeline_output": forecast_step.outputs.job_output_path}


def build_pipeline_job(input_asset_id: str, output_path: str):
    """Instantiate the pipeline job. input_asset_id is the registered MLTABLE asset."""
    # DIRECT mode so the parallel run() receives each partition as a DataFrame.
    pipeline_input = Input(
        type=AssetTypes.MLTABLE,
        path=input_asset_id,
        mode=InputOutputModes.DIRECT,
    )
    job = demand_forecast_pipeline(input_data=pipeline_input)
    job.settings.default_compute = parameters.compute_cluster_name
    # Single concatenated output file (matches v1 append_row).
    job.outputs.pipeline_output.type = AssetTypes.URI_FILE
    if output_path:
        job.outputs.pipeline_output.path = output_path
    return job


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        default="azureml://datastores/workspaceblobdemplan/paths/sampleInput.csv",
        help="Datastore URI of the input CSV (path relative to the container root). "
        "An MLTABLE asset is built around it automatically.",
    )
    parser.add_argument(
        "--output_path",
        default=None,
        help="Optional datastore path for the appended forecast output file.",
    )
    parser.add_argument(
        "--register-component",
        action="store_true",
        help="Register the pipeline as a versioned, reusable component "
        "(v2 analogue of publishing/updating the v1 PipelineEndpoint).",
    )
    parser.add_argument(
        "--no-submit",
        action="store_true",
        help="Build/register only; do not submit a run.",
    )
    args = parser.parse_args()

    ml_client = get_ml_client()

    # Register the R code asset before building the pipeline (the step needs its id).
    global _REGISTERED_CODE
    _REGISTERED_CODE = register_code_asset(ml_client)
    print(f"Registered code asset: {_REGISTERED_CODE}")

    # Wrap the input CSV in an MLTABLE asset for the parallel job's tabular input.
    input_asset_id = register_mltable_asset(ml_client, args.input_path)
    print(f"Registered input MLTable asset: {input_asset_id}")

    default_output = (
        args.output_path
        or "azureml://datastores/workspaceblobdemplan/paths/outputs/"
        + datetime.now().strftime("%Y%m%dT%H%M%S")
    )
    # Expand short datastore URIs to the long form (as for the input).
    default_output = _expand_datastore_uri(ml_client, default_output)

    if args.register_component:
        # v2 analogue of PipelineEndpoint.publish: register a versioned component.
        registered = ml_client.components.create_or_update(
            demand_forecast_pipeline.component
        )
        print(
            f"Registered pipeline component '{registered.name}' "
            f"version '{registered.version}'."
        )

    if args.no_submit:
        return

    job = build_pipeline_job(input_asset_id, default_output)
    submitted = ml_client.jobs.create_or_update(
        job, experiment_name="DemandForecastGeneration_TriggerScript"
    )
    print(f"Submitted pipeline job: {submitted.name}")
    print(f"Studio URL: {submitted.studio_url}")

    # v1 wait_for_completion timeout has no v2 equivalent; wrap stream() if needed.
    ml_client.jobs.stream(submitted.name)


if __name__ == "__main__":
    main()
