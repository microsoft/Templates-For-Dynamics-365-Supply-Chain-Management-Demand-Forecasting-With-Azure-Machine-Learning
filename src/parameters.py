# Amount of nodes from the compute cluster to run the forecast parallel step on.
# On each node, processes_per_node worker processes execute mini-batches in parallel, so the
# effective level of parallelism is nodes_count * processes_per_node.
# In case nodes_count is set higher than available in the cluster the run might be queued until more
# resources are available.
nodes_count = 5

# Worker processes per node (v2 max_concurrency_per_instance, which defaults to 1). Each process
# runs forecast.r single-threaded, so match the cluster VM's core count (STANDARD_DS3_V2 = 4).
processes_per_node = 4

# The input is split into partitions (one mini-batch each) that always keep a forecast series
# (GranularityAttributeKey) whole. Each partition's output is appended to parallel_run_step.txt
# in one storage append (hard limit 4 MiB), so a partition holds at most max_series_per_partition
# series and is further capped from HORIZON to stay below max_partition_output_bytes.
max_series_per_partition = 100
max_partition_output_bytes = 1024 * 1024

# Per mini-batch (partition) timeout in seconds before it is retried / failed.
mini_batch_timeout_seconds = 3600

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
