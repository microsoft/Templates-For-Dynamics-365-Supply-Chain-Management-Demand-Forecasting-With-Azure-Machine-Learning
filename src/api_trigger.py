"""Trigger / publish the demand-forecast pipeline (Azure ML SDK v2).

v2 replaces Workspace.from_config / Experiment.submit / PipelineEndpoint with
MLClient + an @pipeline DSL job + components.create_or_update. The parallel job is
a first-class pipeline step (no nested-pipeline indirection).
"""
import argparse
from datetime import datetime

from azure.ai.ml import MLClient, Input
from azure.ai.ml.constants import AssetTypes
from azure.ai.ml.dsl import pipeline
from azure.identity import DefaultAzureCredential

import parameters
from run import build_parallel_job, build_partition_step, _R_SOURCE_DIR

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
    """Partition the input by forecast series, then forecast each bucket in parallel."""
    partition_step = build_partition_step(
        input_data=input_data, code_asset=_REGISTERED_CODE
    )
    forecast_step = build_parallel_job(
        input_data=partition_step.outputs.partitioned_data,
        code_asset=_REGISTERED_CODE,
    )
    return {"pipeline_output": forecast_step.outputs.job_output_path}


def build_pipeline_job(input_uri: str, output_path: str):
    """Instantiate the pipeline job. input_uri is the datastore URI of the input CSV."""
    pipeline_input = Input(type=AssetTypes.URI_FILE, path=input_uri)
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
        "The pipeline partitions it by forecast series automatically.",
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

    # Expand short datastore URIs to the long form (see _expand_datastore_uri).
    input_uri = _expand_datastore_uri(ml_client, args.input_path)

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

    job = build_pipeline_job(input_uri, default_output)
    submitted = ml_client.jobs.create_or_update(
        job, experiment_name="DemandForecastGeneration_TriggerScript"
    )
    print(f"Submitted pipeline job: {submitted.name}")
    print(f"Studio URL: {submitted.studio_url}")

    # v1 wait_for_completion timeout has no v2 equivalent; wrap stream() if needed.
    ml_client.jobs.stream(submitted.name)


if __name__ == "__main__":
    main()
