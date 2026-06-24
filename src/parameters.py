# Amount of nodes from the compute cluster to run the script in parallel.
# Max value is 10. On each node there will be several worker processes executing the forecasting script in parallel.
# The total number of worker processes in your job is process_count_per_node * node_count, where
# process_count_per_node equals the number of cores a node has. So if the compute cluster has type
# Standard_D4 (8 cores) and max 10 nodes, then the effective level of parallelism is 80; however the
# cluster should have 11 nodes (1 node will be used to initiate the run).
# In case nodes_count is set higher than available in the cluster the run might be queued until more
# resources are available.
nodes_count = 5

# The compute cluster to use for running the pipelines.
compute_cluster_name = "e2ecpucluster"

# Timeout for a pipeline execution.
timeout_seconds = 24 * 3600

# ---------------------------------------------------------------------------
# Workspace connection details (SDK v2).
#
# SDK v2 (azure-ai-ml) no longer reads a workspace config.json the way v1 did
# via Workspace.from_config(). Instead, MLClient is constructed from the
# subscription id, resource group, and workspace name (see api_trigger.py).
#
# Leave these as None to fall back to the values in a local config.json /
# environment, or set them explicitly. They are read by api_trigger.py.
# ---------------------------------------------------------------------------
subscription_id = None
resource_group = None
workspace_name = None
