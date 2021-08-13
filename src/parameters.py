# Amount on nodes from the compute cluster to run the script in parallel. 
# Max value is 10. On each node there will be several worker processes executing forecasting script in parallel.
# The total number of worker processes in your job is process_count_per_node * node_count
# Where process_count_per_node is equal to the number of cores a node has. So if the compute cluster has type Standard_D4 (8 cores) and max 10 nodes,
# then the effective level of parallelism is 80 however the cluster should have 11 nodes (1 node will be used to initiate the run).
# In case nodes_count is set higher than available in the cluster - the run might be queued untill more resources are available.
nodes_count = 5

# The compute cluster to use for running the pipelines. 
compute_cluster_name = "e2ecpucluster"

# Timeout for a pipleine execution.
timeout_seconds = 24*3600