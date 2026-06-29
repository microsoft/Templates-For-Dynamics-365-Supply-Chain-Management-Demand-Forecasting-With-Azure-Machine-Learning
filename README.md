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
