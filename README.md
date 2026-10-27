# Templates for Dynamics 365 Supply Chain Management demand forecasting with Azure Machine Learning

This repository contains template scripts that you can use to set up your Azure environment to generate demand forecasts using the Azure Machine Learning Service, which you can then integrate with the demand forecasting feature of Dynamics 365 Supply Chain Management. For details, see [Demand forecasting overview](https://docs.microsoft.com/dynamics365/supply-chain/master-planning/introduction-demand-forecasting).

> **Note**
> Azure Machine Learning SDK v1 (`azureml-core`) is being retired. These templates have been updated to use **Azure Machine Learning SDK v2** (`azure-ai-ml`). Existing v1 deployments continue to work; for new setups, use the scripts in this repository as described below and in [Demand forecasting setup](https://learn.microsoft.com/dynamics365/supply-chain/master-planning/demand-forecasting-setup).

## Set up your Azure subscription to generate demand forecasts

To enable your Azure subscription to generate demand forecasts, we recommend that you run the `quick_setup.ps1` script included in this repository on your Azure environment and follow the instructions provided by the script. Alternatively, you could set it up manually as described in [Demand forecasting setup](https://go.microsoft.com/fwlink/?linkid=2165514).

## How it works

Supply Chain Management's demand forecasting connector calls an Azure Machine Learning **v1 published-pipeline REST endpoint**. SDK v2 doesn't expose that endpoint type, so this template adds a small **relay Azure Function** (in `relay/`) that accepts the requests Supply Chain Management sends and translates them to the v2 pipeline. `quick_setup.ps1` provisions and deploys everything, then prints the values to enter on the **Demand forecasting parameters** page in Supply Chain Management — including the **Pipeline endpoint address** (the relay URL).

### Repository contents

- `quick_setup.ps1` – idempotent setup: creates or reuses the Azure ML workspace, storage, datastore, and compute, then provisions and deploys the relay Function.
- `src/` – the v2 pipeline: `api_trigger.py` (build/submit), `run.py` (parallel job), `parameters.py`, and `REntryScript/` (the unchanged `forecast.r` plus a Python entry shim, `entry.py` / `forecast_entry.r`, that v2 parallel jobs require).
- `relay/` – the Azure Function that bridges Supply Chain Management to the v2 pipeline.
- `sampleInput.csv` – sample input used to validate the setup.

### Relay Python runtime

`quick_setup.ps1` creates new Linux Consumption relay apps with **Python 3.12** and Functions v4. On every run (including `-RelayOnly`), it inspects the existing app's operating system, hosting plan, worker and runtime. It verifies Python 3.12 before deploying, and uses `az functionapp config set --linux-fx-version "Python|3.12"` to update managed Python apps on supported Premium or Dedicated plans. An update restarts the app; schedule production updates during a maintenance window.

Azure Functions [does not support changing Python versions on existing Linux Consumption apps](https://learn.microsoft.com/azure/azure-functions/update-language-versions?pivots=programming-language-python). If an existing Consumption relay uses Python 3.11, setup stops with an explicit error rather than silently reusing it or claiming an unsupported upgrade succeeded. Provision a new Python 3.12 relay using a different `-functionAppName`, then replace the D365 pipeline endpoint with the newly printed URL after validation. Alternatively, plan a supported hosting migration separately. Setup never deletes the old app. Flex Consumption has a [different deployment contract](https://learn.microsoft.com/azure/azure-functions/flex-consumption-plan#deployment) and is not supported by this script's zip-deploy workflow; Windows, container-based, non-Python and non-v4 apps also require separate migration.

### Complete-history dispatch

The pipeline keeps preparation and forecasting in **one pipeline job**. Its first command step materializes one CSV per `GranularityAttributeKey`, preserving source row order, string values and sparse first-row forecast parameters. It writes an MLTable with `extract_columns_from_partition_format` metadata, following the [official SDK v2 partition-by-key example](https://github.com/Azure/azureml-examples/tree/main/cli/jobs/parallel/1a_oj_sales_prediction). The parallel step consumes this MLTable in `direct` mode with `partition_keys=["GranularityAttributeKey"]`, so an individual key's history is not split by a size-based mini-batch.

D365's integer keys remain strings (including leading zeros). Preparation rejects missing, empty or unsafe path keys rather than silently dropping rows or normalizing distinct keys into the same directory; supported key strings contain letters, digits, `_` or `-`. Preparation loads the source history into memory on one compute node, which must have sufficient memory for the input. The R forecasting model, per-key parameter handling and headerless appended output remain unchanged. The relay still writes to `{output_path}/parallel_run_step.txt`.

Key partitioning reduces the amount returned by an individual parallel call compared with returning all forecasts together. It does **not** establish that every `RequestBodyTooLarge` case is resolved; a large single-key forecast or another service payload can still exceed limits. Validate representative workloads in Azure ML before production use.

### Focused local checks

In a Python 3.12 environment with `azure-ai-ml`, `mltable` and `azureml-dataprep[pandas]` installed, run:

```powershell
python -m unittest discover -s tests -p test_forecast_partitioning.py
pwsh -NoProfile -File .\tests\test_function_runtime.ps1
```

These checks exercise real local MLTable preparation and SDK pipeline serialization, a mocked R process, and mocked Azure CLI runtime scenarios. They do not deploy resources, run R models, or verify cloud dispatch, remote Function builds or production forecasts.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
