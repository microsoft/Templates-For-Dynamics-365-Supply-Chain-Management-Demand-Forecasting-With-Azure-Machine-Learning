"""Relay that lets D365 SCM (which speaks the AML v1 published-pipeline REST
contract via the compiled ForecastClient) trigger the v2 demand-forecast pipeline.

D365's ForecastClient keeps only the AUTHORITY of the MLSPipelineEndpointUri and
rebuilds four fixed URLs against it, so this app serves those four routes:

  POST  /pipelines/v1.0/.../PipelineRuns/PipelineEndpointSubmit/Id/{id}   -> submit
  GET   /history/v1.0/.../runs/{runId}/details                            -> status
  POST  /pipelines/v1.0/.../PipelineRuns/{runId}/Cancel                   -> cancel
  POST  /history/v1.0/.../experiments/{exp}/runs:query                    -> query

Data flows through blob storage (container demplan-azureml): D365 uploads the
merged input CSV and reads {output_path}/parallel_run_step.txt, so the submit
handler points the v2 job's output at that exact file.

The forecast pipeline itself is unchanged -- this reuses api_trigger's builders.
"""
import json
import logging
import os
import re
import sys
import threading

import azure.functions as func

# The v2 pipeline code (api_trigger.py, run.py, parameters.py, REntryScript/) is
# deployed under ./src so the builders can be reused as-is.
_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Datastore that points at the demplan-azureml container (created by quick_setup).
_DATASTORE = os.environ.get("RELAY_DATASTORE", "workspaceblobdemplan")
_SUBSCRIPTION = os.environ.get("AML_SUBSCRIPTION_ID")
_RESOURCE_GROUP = os.environ.get("AML_RESOURCE_GROUP")
_WORKSPACE = os.environ.get("AML_WORKSPACE_NAME")
# Optional caller validation: when both are set, the incoming AAD token must be
# from this tenant and issued to this app (the D365 service principal).
_EXPECTED_TENANT = os.environ.get("RELAY_EXPECTED_TENANT_ID")
_EXPECTED_APP_ID = os.environ.get("RELAY_EXPECTED_APP_ID")

_ml_client = None
_code_asset = None
_lock = threading.Lock()
_jwk_client = None


class _Unauthorized(Exception):
    pass


def _json(obj, status=200):
    return func.HttpResponse(json.dumps(obj), status_code=status, mimetype="application/json")


def _error(exc):
    # Log the full exception (App Insights) and return a short error to the caller.
    logging.exception("relay handler failed")
    return _json({"error": str(exc)}, 500)


def _client():
    global _ml_client
    if _ml_client is None:
        with _lock:
            if _ml_client is None:
                from azure.ai.ml import MLClient
                from azure.identity import DefaultAzureCredential

                _ml_client = MLClient(
                    DefaultAzureCredential(),
                    _SUBSCRIPTION,
                    _RESOURCE_GROUP,
                    _WORKSPACE,
                )
    return _ml_client


def _validate_token(req):
    """Verify the caller's AAD bearer token (issuer tenant + app id). The token's
    audience is AML, not this app, so audience is not checked. Fails closed: if the
    expected tenant/app id are not configured, the request is rejected."""
    if not (_EXPECTED_TENANT and _EXPECTED_APP_ID):
        raise _Unauthorized(
            "Caller validation is not configured. Set the RELAY_EXPECTED_TENANT_ID "
            "and RELAY_EXPECTED_APP_ID app settings to the D365 service principal's "
            "tenant id and application id."
        )

    auth = req.headers.get("Authorization", "")
    if not auth.lower().startswith("bearer "):
        raise _Unauthorized("Missing bearer token.")
    token = auth[7:].strip()

    import jwt
    from jwt import PyJWKClient

    global _jwk_client
    if _jwk_client is None:
        _jwk_client = PyJWKClient(
            f"https://login.microsoftonline.com/{_EXPECTED_TENANT}/discovery/v2.0/keys"
        )
    try:
        signing_key = _jwk_client.get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token, signing_key, algorithms=["RS256"], options={"verify_aud": False}
        )
    except Exception as exc:  # noqa: BLE001
        raise _Unauthorized(f"Invalid token: {exc}")

    if _EXPECTED_TENANT not in claims.get("iss", ""):
        raise _Unauthorized("Token from unexpected tenant.")
    caller_app = claims.get("appid") or claims.get("azp")
    if caller_app != _EXPECTED_APP_ID:
        raise _Unauthorized("Token issued to unexpected application.")


def _map_status(aml_status):
    """Map an AML job status to the codes ForecastClient understands."""
    status = (aml_status or "").lower()
    if status == "completed":
        return "COMPLETED"
    if status == "failed":
        return "FAILED"
    if status in ("canceled", "cancelled", "cancelrequested"):
        return "CANCELED"
    return "RUNNING"


def _submit(req):
    try:
        body = req.get_json()
    except ValueError:
        body = json.loads(req.get_body() or b"{}")
    experiment = (
        body.get("ExperimentName")
        or body.get("experimentName")
        or "DemandForecastGeneration"
    )
    params = body.get("ParameterAssignments") or body.get("parameterAssignments") or {}
    input_path = params.get("input_path")
    output_path = params.get("output_path")
    if not input_path or not output_path:
        return _json({"error": "input_path and output_path are required."}, 400)

    ml = _client()
    import api_trigger

    # Register the R code asset once per worker; the @pipeline reads the module global.
    global _code_asset
    if _code_asset is None:
        _code_asset = api_trigger.register_code_asset(ml)
    api_trigger._REGISTERED_CODE = _code_asset

    input_uri = f"azureml://datastores/{_DATASTORE}/paths/{input_path}"
    # read_into_memory=False -> the MLTable references the blob directly (no slow
    # synchronous read). register=False -> inline input, so no data-asset version is
    # created per forecast.
    input_asset = api_trigger.register_mltable_asset(
        ml, input_uri, read_into_memory=False, register=False
    )

    # ForecastClient reads {output_path}/parallel_run_step.txt, so target that file.
    output_uri = (
        f"azureml://datastores/{_DATASTORE}/paths/"
        f"{output_path.rstrip('/')}/parallel_run_step.txt"
    )
    job = api_trigger.build_pipeline_job(input_asset, output_uri)
    submitted = ml.jobs.create_or_update(job, experiment_name=experiment)

    logging.info("Submitted pipeline job %s (experiment %s)", submitted.name, experiment)
    return _json({"Id": submitted.name})


def _status(run_id):
    job = _client().jobs.get(run_id)
    return _json({"status": _map_status(getattr(job, "status", None))})


def _cancel(run_id):
    try:
        _client().jobs.begin_cancel(run_id)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Cancel of %s failed: %s", run_id, exc)
    return func.HttpResponse(status_code=200)


def _query(experiment):
    """Return the experiment's active (non-terminal) parent runs so ForecastClient
    cancels them before submitting -- matching the v1 endpoint's behaviour. Scans
    only the most recent jobs to bound the cost (an in-flight run is recent)."""
    terminal = ("completed", "failed", "canceled", "cancelled")
    active = []
    scanned = 0
    for job in _client().jobs.list():
        scanned += 1
        if scanned > 200:
            break
        if getattr(job, "experiment_name", None) != experiment:
            continue
        if getattr(job, "parent_job_name", None):
            continue
        if (getattr(job, "status", "") or "").lower() in terminal:
            continue
        active.append({"runId": job.name})
    return _json({"value": active})


app = func.FunctionApp()


@app.route(route="pipelines/v1.0/{*path}", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def pipelines(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _validate_token(req)
    except _Unauthorized as exc:
        return _json({"error": str(exc)}, 401)

    path = req.route_params.get("path", "")
    try:
        if re.search(r"/PipelineEndpointSubmit/Id/[^/]+$", path):
            return _submit(req)
        cancel = re.search(r"/PipelineRuns/([^/]+)/Cancel$", path)
        if cancel:
            return _cancel(cancel.group(1))
        return _json({"error": f"Unmatched pipelines route: {path}"}, 404)
    except Exception as exc:  # noqa: BLE001
        return _error(exc)


@app.route(route="history/v1.0/{*path}", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def history(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _validate_token(req)
    except _Unauthorized as exc:
        return _json({"error": str(exc)}, 401)

    path = req.route_params.get("path", "")
    try:
        details = re.search(r"/runs/([^/]+)/details$", path)
        if details and req.method == "GET":
            return _status(details.group(1))
        query = re.search(r"/experiments/([^/]+)/runs:query$", path)
        if query and req.method == "POST":
            return _query(query.group(1))
        return _json({"error": f"Unmatched history route: {path}"}, 404)
    except Exception as exc:  # noqa: BLE001
        return _error(exc)
