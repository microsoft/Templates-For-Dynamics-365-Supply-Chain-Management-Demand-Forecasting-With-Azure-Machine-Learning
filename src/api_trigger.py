from datetime import datetime
from azureml.data.datapath import DataPath, DataPathComputeBinding
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import PipelineParameter, PipelineEndpoint
from azureml.pipeline.core import Pipeline, PipelineData
from azureml.core import Workspace, Experiment
from azureml.core.compute import AmlCompute
from azureml.core.runconfig import RunConfiguration
from azureml.pipeline.core._restclients.aeva.models.error_response import ErrorResponseException
import parameters

workspace = Workspace.from_config()
datastore = workspace.get_default_datastore()

# Parameters are passed to the API as a part of submit REST request during a forecasting run.
input_path_pipeline_param = PipelineParameter(name="input_path", default_value="sampleInput.csv")
output_path_pipeline_param = PipelineParameter(name="output_path", default_value="output")
rc = RunConfiguration()

# Add a python script step which starts a parallel run and fans out granularity attributes for processing.
run_step = PythonScriptStep(
    script_name="./run.py",
    source_directory='.',
    arguments=['--input_path', input_path_pipeline_param, '--output_path', output_path_pipeline_param],
    compute_target = AmlCompute(workspace, parameters.compute_cluster_name),
    runconfig = rc,
    allow_reuse = False)
pipeline = Pipeline(workspace=workspace, steps=[run_step])
pipelineName = 'TriggerDemandForecastGeneration'

# API throws an error in case endpoint does not yet exist, no way to check for existence as of now.
try:
    pipelineEndpoint = PipelineEndpoint.get(workspace=workspace, name=pipelineName)
except ErrorResponseException as ex:
    if "not found in workspace" in ex.message:
        pipelineEndpoint = None
    else:
        raise


experiment = Experiment(workspace, 'DemandForecastGeneration_TriggerScript')
submitParameters = {"input_path": 'sampleInput.csv', "output_path": 'outputs/'+ datetime.now().strftime("%Y%m%dT%H%M%S")}

# The block below will either publish new pipeline endpoint or update existing.
# Pipeline endpoint update is needed in case underlying run.py or forecasting.r scripts are updated.
if pipelineEndpoint is None:
    print('Pipeline does not exists, creating new: ' + pipelineName)
    pipelineEndpoint = PipelineEndpoint.publish(workspace = workspace, name = pipelineName, pipeline=pipeline, description="Initiate demand forecast generation.")
else:
    print('Found existing pipeline ' + pipelineName + ', adding new version.')
    published_pipeline = pipeline.publish(name = pipelineName + "_Pipeline")
    pipelineEndpoint.add_default(published_pipeline)

# This can be commented out as actual run is not needed to update the pipeline endpoint however is a good way to ensure the setup is correct.
pipeline_run = experiment.submit(pipelineEndpoint, pipeline_parameters=submitParameters)

pipeline_run.wait_for_completion(show_output=True, timeout_seconds=parameters.timeout_seconds)