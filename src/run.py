import os
os.system(f"pip install azureml")
os.system(f"pip install azureml-core")
os.system(f"pip install azureml-data")
os.system(f"pip install azureml-pipeline")
os.system(f"pip install argparse")
os.system(f"pip install azureml-dataset-runtime[fuse,pandas]")

from datetime import datetime
import sys
import argparse
from azureml.data import OutputFileDatasetConfig
from azureml.core import Environment, Workspace, Experiment, Run
from azureml.core.compute import AmlCompute
from azureml.core.dataset import Dataset
from azureml.core.datastore import Datastore
from azureml.core.environment import DEFAULT_CPU_IMAGE
from azureml.exceptions import ComputeTargetException, ExperimentExecutionException
from azureml.pipeline.core import Pipeline, PipelineData
from azureml.pipeline.steps import ParallelRunConfig, ParallelRunStep
from azureml.pipeline.core import PipelineParameter
import parameters

def get_input(workspace, inputPath):
    """Return an input."""
    file_datastore = workspace.get_default_datastore()
    file_datasetAggregated = Dataset.Tabular.from_delimited_files(path=(file_datastore, inputPath))
    partitioned_dataset = file_datasetAggregated.partition_by(partition_keys=['GranularityAttributeKey'], target=(file_datastore, "partition_by_GranularityAttributeKey"), name="partitioned_historical_data")

    return partitioned_dataset.as_named_input("partitioned_tabular_input")

def get_output(workspace, outputPath):
    # Each node from the parallel run will output lines that are concatenated into 1 file (output data from each minibatch).
    # Output data from each minibatch is sequential.
    output_name = "parallelRunOutput"
    return OutputFileDatasetConfig(name=output_name, destination=(workspace.get_default_datastore(), outputPath))

def r_env():
    # Return environment for R.
    env = Environment("parallel_run_step")
    env.python.user_managed_dependencies = True
    env.docker.base_image = None  # Set base image to None, because the image is defined by dockerfile.
    env.docker.base_dockerfile = f"""
FROM {DEFAULT_CPU_IMAGE}

# Pin pip version to avoid known ruamel installation issue.
# https://docs.microsoft.com/en-us/python/api/overview/azure/ml/install?view=azure-ml-py#troubleshooting
RUN conda install -c r -y pip=20.1.1

# For dataprep.
RUN pip install azureml-core azureml-dataset-runtime

# Setup r runtime. Ref: http://cran.rstudio.com/bin/linux/ubuntu/
  RUN apt update -qq -y \
  && apt install --no-install-recommends software-properties-common dirmngr -y \
  && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
  && add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/" \
  && add-apt-repository ppa:c2d4u.team/c2d4u4.0+ \
  && apt-get update \
  && apt install r-base r-base-dev littler curl r-cran-curl -y --allow-unauthenticated

#su
#wget https://github.com/curl/curl/releases/download/curl-7_55_0/curl-7.55.0.tar.gz
#./configure
#make 
#make install

# Install packages used by r agent.
RUN r -e 'install.packages(c("funr", "jsonlite", "logging"))'
RUN r -e 'install.packages("forecast",repos = "http://cran.us.r-project.org")'
RUN r -e 'install.packages("plyr",repos = "http://cran.us.r-project.org")'
RUN r -e 'install.packages("zoo",repos = "http://cran.us.r-project.org")'
"""

    return env


def runParallel(inputPath, outputPath):
    workspace = Run.get_context().experiment.workspace
    
    # The cluster name to use for running will be forced by a setup script, adjust if using another cluster
    try:
        cluster = AmlCompute(workspace, parameters.compute_cluster_name)
        print("found existing cluster.")
    except ComputeTargetException:
        # Don't create the cluster automatically to avoid leaving it silently in the workspace.
        raise Exception(f"Compute {parameters.compute_cluster_name} is not found in workspace {workspace.name}.")

    parallel_run_config = ParallelRunConfig(
        environment=r_env(),
        entry_script="forecast.R",  # demand forecast generation script to run against each input
        partition_keys=['GranularityAttributeKey'],
        error_threshold=-1, # parallel run checks output rows count vs input and throws error if output < input, we need to disable this behavior.
        output_action="append_row", # output of each minibatch run is concatenated into 1 txt file with no column header.
        source_directory="REntryScript",
        description="Generate demand forecast.",
        compute_target=cluster,
        node_count=parameters.nodes_count,
        allowed_failed_count=0, # fail the run if at least 1 minibatch processing fails, this is needed to compensate error_threshold set to -1
        logging_level="DEBUG",
    )
        
    # 3-32 chars with ^[a-z]([-a-z0-9]*[a-z0-9])?$ regex
    # To re-use an existing snapshot, change the step_name to an existing name.
    #step_name = f'step{datetime.utcnow().strftime("%y%m%d-%H%M")}'
    step_name = 'r-forecast'
    step = ParallelRunStep(
        name=step_name,
        inputs=[get_input(workspace, inputPath)],
        output=get_output(workspace, outputPath),
        arguments=[],
        parallel_run_config=parallel_run_config,
        allow_reuse=False,
    )

    # Start a sub pipeline run.
    pipeline = Pipeline(workspace=workspace, steps=[step])
    experiment_name = "DemandForecastGeneration_ParallelRun"
    run = Experiment(workspace, experiment_name).submit(pipeline)
    print(f"Experiment name: {experiment_name}, Run id: {run.id}")
    try:
        run.wait_for_completion(show_output=True, timeout_seconds=parameters.timeout_seconds)
        
        status = run.get_status()
        if status not in ["Completed", "Failed", "Canceled", "Finished"]:
            run.cancel()
            raise TimeoutError(
                f"The run {run.id} cannot finish within {parameters.timeout_seconds} seconds. Its current status is "
                f'"{status}". Cancelled it so that the test case can stop. Please try run the test case again.'
            )

    except ExperimentExecutionException as ex:
        if "run interrupted" in ex.message:
            print(f"Received interrupt, cancel the run {run.id}")
            run.cancel()
            sys.exit(1)

try:
    start_time = datetime.now()
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", help="input data")
    parser.add_argument("--output_path", help="output data")
    args = parser.parse_args()
    print(f"Starting parallel run from {args.input_path} file.")
    runParallel(args.input_path, args.output_path)
finally:
    delta = datetime.now() - start_time
    print("Elapsed time: ", delta)
