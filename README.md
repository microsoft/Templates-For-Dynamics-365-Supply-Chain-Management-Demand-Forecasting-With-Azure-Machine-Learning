# Templates for Dynamics 365 Supply Chain Management demand forecasting with Azure Machine Learning

This repository contains template scripts that you can use to set up your Azure environment to generate demand forecasts using the Azure Machine Learning Service, which you can then integrate with the demand forecasting feature of Dynamics 365 Supply Chain Management. For details, see [Demand forecasting overview](https://docs.microsoft.com/dynamics365/supply-chain/master-planning/introduction-demand-forecasting).

> **Note**
> Azure Machine Learning SDK v1 (`azureml-core`) is being retired. These templates have been updated to use **Azure Machine Learning SDK v2** (`azure-ai-ml`). Existing v1 deployments continue to work; for new setups, use the scripts in this repository as described below and in [Demand forecasting setup](https://learn.microsoft.com/dynamics365/supply-chain/master-planning/demand-forecasting-setup).

## Set up your Azure subscription to generate demand forecasts

To enable your Azure subscription to generate demand forecasts, we recommend that you run the `quick_setup.ps1` script included in this repository on your Azure environment and follow the instructions provided by the script. Alternatively, you could set it up manually as described in [Demand forecasting setup](https://go.microsoft.com/fwlink/?linkid=2165514).

### Prerequisites

Use Windows PowerShell 5.1 or PowerShell 7 with Azure CLI and its `ml` v2 extension installed. Your Azure account needs permission to manage the target resources and role assignments; a new installation also requires permission to create an Entra application and service principal.

**Choose one walkthrough below and run `quick_setup.ps1` once.** Setup discovers the token audience, configures the relay, and deploys it. No separate discovery script is needed.

Setup prompts for your subscription ID, resource group, Azure region, forecast storage account, AML workspace, Function app name, and separate Function runtime storage account. Enter chosen resource names for a new installation or the existing values for an update. The Function app name is an Azure resource name, not an Entra application name or client ID.

### Option 1: New installation

1. From the repository root, run the command below and enter the requested resource values. At the application menu, select **1** (create a new Entra application) and enter a display name that does not already exist:

```powershell
.\quick_setup.ps1
```

2. When setup prints the tenant and application client IDs and pauses, leave it running. Open [Azure portal](https://portal.azure.com), then **Settings > Directories + subscriptions** and switch to the printed tenant. Use the top search to open **App registrations > All applications**, search for the printed application (client) ID, and select the matching app. Open **Certificates & secrets > Client secrets > New client secret**, enter a description, choose an expiry according to your organization's policy, and select **Add**. Copy the secret **Value** (not **Secret ID**), retain it securely for D365, and enter it at setup's secure prompt. Setup then continues automatically; it does not create or store the secret.

3. After successful deployment, enter the printed tenant/client IDs, storage account name, **Pipeline endpoint address**, and the same secret Value on D365's **Demand forecasting parameters** page. Follow the [D365 setup instructions](https://go.microsoft.com/fwlink/?linkid=2165514), then run a small forecast to confirm the connection.

Keep the printed client ID for full-setup reruns: select **2** (use an existing Entra application) and enter that **Application (client) ID**, not an object ID, including after interrupted setup. You can also supply it with `-AADApplicationId`. Secret Values cannot be retrieved from Entra after creation.

### Option 2: Update an existing relay

1. Obtain the application client ID and active secret already used by D365 from your integration administrator. Run the command below, enter the existing resource values, and provide that application client ID when prompted:

```powershell
.\quick_setup.ps1 -RelayOnly
```

2. Enter that application's active secret at the secure prompt. Setup discovers the audience and updates the relay in the same run.

3. After successful deployment, run a small forecast in D365. When updating the same Function app with the same resource parameters, the existing D365 pipeline endpoint remains unchanged.

The AML workspace and storage must already exist. The caller tenant defaults to the subscription tenant; if D365 uses a different tenant, add `-d365TenantId "<d365-caller-tenant-id>"` to the command. Do not pass either `-AADApplicationName` or `-AADApplicationId` with `-RelayOnly`. Existing Function apps keep their current Python version.

**For either option:** Wait for the current deployment to complete successfully in the Function app's **Deployment Center** before testing D365. An HTTP `202 Accepted` or a printed endpoint alone does not confirm completion. Enter secrets only at the secure prompt in a trusted session; never put secrets or bearer tokens in logs.

**Optional:** All inputs can still be supplied as named parameters. Add `-d365TokenAudience "<verified-token-audience>"` to skip audience discovery and the secret prompt. For unattended input, also supply every required resource parameter and either `-AADApplicationName`/`-AADApplicationId` for full setup or `-d365AppId` for relay-only setup; Azure CLI login requirements still apply. The audience must be the exact token audience, including any trailing slash, not a client ID, Function URL, or `/.default` scope.

## How it works

Supply Chain Management's demand forecasting connector calls an Azure Machine Learning **v1 published-pipeline REST endpoint**. SDK v2 doesn't expose that endpoint type, so this template adds a small **relay Azure Function** (in `relay/`) that accepts the requests Supply Chain Management sends and translates them to the v2 pipeline. `quick_setup.ps1` provisions and deploys everything, then prints the values to enter on the **Demand forecasting parameters** page in Supply Chain Management — including the **Pipeline endpoint address** (the relay URL).

### Repository contents

- `quick_setup.ps1` – idempotent setup: creates or reuses the Azure ML workspace, storage, datastore, and compute, then provisions and deploys the relay Function.
- `src/` – the v2 pipeline: `api_trigger.py` (build/submit), `run.py` (parallel job), `parameters.py`, and `REntryScript/` (the unchanged `forecast.r` plus a Python entry shim, `entry.py` / `forecast_entry.r`, that v2 parallel jobs require).
- `relay/` – the Azure Function that bridges Supply Chain Management to the v2 pipeline.
- `sampleInput.csv` – sample input used to validate the setup.

New relay Function apps use Python 3.12; rerunning setup does not change an existing app's Python version. The pipeline prepares per-key history files and MLTable partition metadata before parallel dispatch; it materializes the input table in memory on AML compute. Validate cloud dispatch before production use; partitioning alone does not establish that `RequestBodyTooLarge` is resolved.

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
