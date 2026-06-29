<#
.DESCRIPTION
    This script has the following dependencies:
       * a pre-installed Azure CLI (not the PowerShell one).
       * a pre-installed ML extension ml (v2).
    
    Creates Azure resources required by demand forecast logic for FinOps.
    * Creates a resource group.
    * Creates a storage account
    * Creates and inits an ML workspace:
        Creates a datastore for the workspace.
        Creates a compute instance for script run.
        Creates a compute cluster for pipeline run.
    * Creates a service principal used to access created resources.

#>
[CmdletBinding(DefaultParameterSetName = 'Full')]
param (
    [Parameter(Mandatory)]
    [string]
    $subscriptionId,

    [Parameter(Mandatory)]
    [string]
    $resourceGroupName,

    [Parameter(Mandatory)]
    [string]
    $location,

    [Parameter(Mandatory)]
    [string]
    $storageAccountName,
    
    [Parameter(Mandatory)]
    [string]
    $workspaceName,

    [Parameter(Mandatory, ParameterSetName = 'Full')]
    [string]
    $AADApplicationName,

    # Relay (Azure Function) that bridges D365's v1 pipeline-endpoint contract to v2.
    [Parameter(Mandatory)]
    [string]
    $functionAppName,

    [Parameter(Mandatory)]
    [string]
    $functionStorageAccountName,

    # When set, the relay validates the D365 caller's token (tenant + app id).
    [string]
    $d365TenantId = "",

    [string]
    $d365AppId = "",

    # Re-deploy only the relay (idempotent); skips the AML workspace setup.
    [Parameter(ParameterSetName = 'RelayOnly')]
    [switch]
    $RelayOnly
)

$storageContainer = "demplan-azureml"  #should not be changed
$computeCluster_Name = "e2ecpucluster" #should not be changed
$workspaceBlobDS = "workspaceblobdemplan"

function Set-SubscriptionContext{
    # check the subscription
    $res = az account list --query "[?id=='$subscriptionId'].id" --output tsv
    if ([string]::IsNullOrEmpty($res)) {
        Write-Warning "Subscription $subscriptionId is not valid for the current account."
        throw
    }

    # set current subscription context
    az account set --subscription $subscriptionId
}

function Check-InputParams {
    # The script is idempotent (each resource is created only if missing), so only
    # validate the region here; existing resources are reused rather than rejected.
    $res = az account list-locations --query "[?name == '$location'].name" --output tsv
    if ([string]::IsNullOrEmpty($res)) {
        Write-Warning "Azure region $location is not valid for the current account."
        throw
    }
}

function Create-ResourceGroup() {
    if ((az group exists --resource-group $resourceGroupName) -eq "true") {
        Write-Host "Resource group $resourceGroupName already exists."
        return
    }
    # create a resource group
    Write-Host "Creating a resource group $resourceGroupName in $location ..."
    $res = az group create --resource-group $resourceGroupName --location $location

    if ($LASTEXITCODE -ne 0) {
        # failed creating resource group
        Write-Warning "Error while trying to create a resource group."
        throw $res
    }
}

function Create-StorageAccount() {
    $storageSKU = "Standard_LRS"
    $storageAccessTier = "Hot"
    $ruleName = 'Autodeletion'
    $BlobDeleteAfterDaysNumber = 15

    # create the storage account (only if missing)
    $existingId = az storage account show --name $storageAccountName --resource-group $resourceGroupName --query "id" --output tsv 2>$null
    if ($existingId) {
        Write-Host "Storage account $storageAccountName already exists."
        $script:storageAccountId = $existingId
    }
    else {
        Write-Host "Creating a storage account $storageAccountName in $location ..."
        $responseObj = az storage account create --name $storageAccountName --resource-group $resourceGroupName --location $location --sku $storageSKU --access-tier $storageAccessTier `
            | ConvertFrom-Json

        if ($LASTEXITCODE -ne 0) {
            # failed creating ML storage account
            Write-Warning "Error while trying to create a storage account."
            throw $responseObj
        }

        $script:storageAccountId = $responseObj.id
    }

    $script:storageAccessKey = az storage account keys list --resource-group $resourceGroupName --account-name $storageAccountName --query "[0].value" --output tsv

    # create a BLOB container used by forecast logic
    $res = az storage container create --name $storageContainer --account-name $storageAccountName --resource-group $resourceGroupName --account-key $storageAccessKey
    if ($LASTEXITCODE -ne 0) {
        # failed creating BLOB container
        Write-Warning "Error while trying to create a BLOB container."
        throw $res
    }

    # create a lifetime policy to auto delete blob in the forecast container
    $rule = @{ definition = @{actions = @{baseBlob = @{delete = @{daysAfterModificationGreaterThan = $BlobDeleteAfterDaysNumber}}}; filters = @{blobTypes = ,"blockBlob"; prefixMatch = ,$storageContainer}}; type = "Lifecycle"; name = $ruleName; enabled = $true;}
    $jsonPolicy = @{ rules = ,$rule; } | ConvertTo-json -Depth 10 -Compress
    $jsonPolicy = $jsonPolicy -replace '"', '\"'

    $res = az storage account management-policy create --account-name $storageAccountName  --resource-group $resourceGroupName --policy $jsonPolicy
    if ($LASTEXITCODE -ne 0) {
        # failed creating storage account lifetime policy
        Write-Warning "Error while trying to create a lifetime policy for a storage account."
        throw $res
    }
}

function Create-DataStoreConfigFile() {
    $filePath = ".\wsBlobDSDefinition.yml"

    $res = New-Item -Path $filePath -ItemType File

    $configData = 
@"
        `$schema: https://azuremlschemas.azureedge.net/latest/azureBlob.schema.json
        name: $workspaceBlobDS
        type: azure_blob
        description: Datastore with blob container used by demand planning ML.
        account_name: $storageAccountName
        container_name: $storageContainer
        credentials:
            account_key: $storageAccessKey
"@

    #write the config data
    Set-Content $filePath $configData
    return $filePath
}

function Create-Workspace() {
    $existingWs = az ml workspace show --name $workspaceName --resource-group $resourceGroupName --query "id" --output tsv 2>$null
    if ($existingWs) {
        Write-Host "ML workspace $workspaceName already exists."
        $script:workspaceId = $existingWs
    }
    else {
        # create ML workspace
        Write-Host "Creating an ML workspace $workspaceName ..."
        $responseObj = az ml workspace create --name $workspaceName --resource-group $resourceGroupName --storage-account $storageAccountId `
            | ConvertFrom-Json

        if ($LASTEXITCODE -ne 0) {
            # failed creating ML workspace
            Write-Warning "Error while trying to create an ML workspace."
            throw $responseObj
        }

        $script:workspaceId = $responseObj.id
    }

    # ensure the datastore exists (only if missing)
    $existingDs = az ml datastore show --name $workspaceBlobDS --resource-group $resourceGroupName --workspace-name $workspaceName --query "name" --output tsv 2>$null
    if ($existingDs) {
        Write-Host "Datastore $workspaceBlobDS already exists."
        return
    }

    $dsConfigurationFileName = Create-DataStoreConfigFile 

    try {
        Write-Host "Setting up an ML workspace datastore ..."
        $res = az ml datastore create --file $dsConfigurationFileName --resource-group $resourceGroupName --workspace-name $workspaceName --subscription $subscriptionId

        if ($LASTEXITCODE -ne 0) {
            # failed attaching datastore
            Write-Warning "Error while trying to create an ML workspace datastore."
            throw $res
        }
    }
    finally {
        if  ($dsConfigurationFileName -And (Test-Path $dsConfigurationFileName)) {
            Remove-Item $dsConfigurationFileName
        }
    }
}

function Create-ComputeInstance() {
    $computeInstance_vm_size = "Standard_D3_v2"
    $defaultIdleTimeForShutdownInMin = 60

    # Skip if a compute instance already exists (avoid duplicates on re-run).
    $types = az ml compute list --resource-group $resourceGroupName --workspace-name $workspaceName --query "[].type" --output tsv 2>$null
    if ($types -and (($types -split "`n") | Where-Object { $_.Trim().ToLower() -eq "computeinstance" })) {
        Write-Host "An ML compute instance already exists; skipping."
        return
    }

    $date = Get-Date
    $timePortion = $date.ToString("MMddhhmmss")  # 10 chars
    $baseInstance_Name = "scriptExecutor"        # 14 chars

    #as of now the name length is limited with 24 characters
    $uniqueInstanceName = $baseInstance_Name + $timePortion

    # create workspace compute instance
    Write-Host "Creating an ML workspace compute instance ..."
    $res = az ml compute create --type computeinstance --name $uniqueInstanceName --resource-group $resourceGroupName `
        --workspace-name $workspaceName --size $computeInstance_vm_size --set "idle_time_before_shutdown_minutes=$defaultIdleTimeForShutdownInMin"

    if ($LASTEXITCODE -ne 0) {
        # failed creating workspace compute instance
        Write-Warning "Error while trying to create an ML compute instance."
        throw $res
    }
}

function Create-ComputeCluster() {
    $existing = az ml compute show --name $computeCluster_Name --resource-group $resourceGroupName --workspace-name $workspaceName --query "name" --output tsv 2>$null
    if ($existing) {
        Write-Host "Compute cluster $computeCluster_Name already exists."
        return
    }

    $computeCluster_min_nodes = 0
    $computeCluster_max_nodes = 6 
    $computeCluster_vm_size = "STANDARD_DS3_V2"

    # create workspace compute cluster
    Write-Host "Creating an ML workspace compute cluster ..."
    $res = az ml compute create --type amlcompute --name $computeCluster_Name --resource-group $resourceGroupName --workspace-name $workspaceName `
        --min-instances $computeCluster_min_nodes --max-instances $computeCluster_max_nodes --size $computeCluster_vm_size

    if ($LASTEXITCODE -ne 0) {
        # failed creating compute cluster
        Write-Warning "Error while trying to create an ML compute cluster."
        throw $res
    }
}

function Create-RoleForScope([string]$assignee, [string] $role, [string] $scope) {
    $res = az role assignment create --assignee $assignee --role $role --scope $scope

    if ($LASTEXITCODE -ne 0) {
        # failed assign a role
        Write-Warning "Error while trying to set up a role $role for $scope."
        throw $res
    }
}

function Create-AppWithPrincipal{
    # create the AAD application (only if missing)
    $existingAppId = az ad app list --display-name $AADApplicationName --query "[0].appId" --output tsv
    if ($existingAppId) {
        Write-Host "AAD application $AADApplicationName already exists."
        $script:appId = $existingAppId
    }
    else {
        Write-Host "Creating an AAD application for forecast access ..."
        $responseObj = az ad app create --display-name $AADApplicationName | ConvertFrom-Json

        if ($LASTEXITCODE -ne 0) {
            # failed creating AAD application
            Write-Warning "Error while trying to create an AAD application."
            throw $responseObj
        }

        $script:appId = $responseObj.appId
    }

    # ensure a service principal exists for the app (only if missing)
    $existingSp = az ad sp list --filter "appId eq '$appId'" --query "[0].id" --output tsv
    if ($existingSp) {
        Write-Host "Service principal for $AADApplicationName already exists."
    }
    else {
        Write-Host "Creating a service principal for an application $AADApplicationName ..."
        $res = az ad sp create --id $appId

        if ($LASTEXITCODE -ne 0) {
            # failed creating service principal
            Write-Warning "Error while trying to create a service principal."
            throw $res
        }
    }

    # create security roles (idempotent)
    Ensure-RoleAssignment $appId "Contributor" $workspaceId
    Ensure-RoleAssignment $appId "Contributor" $storageAccountId
    Ensure-RoleAssignment $appId "Storage Blob Data Contributor" $storageAccountId
}

function Get-RelayEndpoint {
    # Only the authority of this URL is used by D365; the path segments are parsed
    # for subscription / resource group / workspace / Id, so they must all be present.
    $fqdn = az functionapp show --name $functionAppName --resource-group $resourceGroupName --query defaultHostName --output tsv
    return "https://$fqdn/pipelines/v1.0/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.MachineLearningServices/workspaces/$workspaceName/PipelineRuns/PipelineEndpointSubmit/Id/relay"
}

function Display-ScriptResult{
    $azureTenantId = az account show --query "tenantId" --output tsv
    $documentationLink = "https://go.microsoft.com/fwlink/?linkid=2165514"

    Write-Host "`nDemand forecast setup script has completed."
    Write-Host "Please create an application secret explicitly and proceed with workspace pipeline configuration according to public documentation: $documentationLink"
    
    # display information needed by FinOps for AML demand forecast
    Write-Host "`nDemand forecast parameters (enter these on the D365 Demand forecasting parameters page).`n"
    Write-Host "Azure tenant id: $azureTenantId"
    Write-Host "Storage account name: $storageAccountName"
    Write-Host "Application id: $appId"
    Write-Host "Pipeline endpoint address (MLSPipelineEndpointUri): $(Get-RelayEndpoint)"
}

function Check-AzureClientVersion {
    $isAzInstalled = Get-Command az -ErrorAction SilentlyContinue
    if (-not $isAzInstalled) {
        throw "AZ CLI is not installed, please go get it at: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    }

    $urlToSetupMLExtensionV2 = "https://learn.microsoft.com/en-us/azure/machine-learning/how-to-configure-cli?view=azureml-api-2"

    $mlExtensionV1 =  az extension list --query "[?name=='azure-cli-ml'].name" --output tsv
    if ($mlExtensionV1) {
        throw "Outdated version of ml extension (v1) for AZ CLI is installed. Please deinstall it and install ml extension (v2), see: $urlToSetupMLExtensionV2"	
    }

    $mlExtensionV2 =  az extension list --query "[?name=='ml'].name" --output tsv
    if (!$mlExtensionV2) {
        throw "ML extension (v2) for AZ CLI is not installed. Please install it, see: $urlToSetupMLExtensionV2"
    }
}

function Ensure-FunctionStorage {
    $exists = az storage account show --name $functionStorageAccountName --resource-group $resourceGroupName --query "name" --output tsv 2>$null
    if ($exists) {
        Write-Host "Function runtime storage account $functionStorageAccountName already exists."
        return
    }
    Write-Host "Creating a Function runtime storage account $functionStorageAccountName ..."
    $res = az storage account create --name $functionStorageAccountName --resource-group $resourceGroupName --location $location --sku Standard_LRS
    if ($LASTEXITCODE -ne 0) { throw $res }
}

function Ensure-FunctionApp {
    $exists = az functionapp show --name $functionAppName --resource-group $resourceGroupName --query "name" --output tsv 2>$null
    if ($exists) {
        Write-Host "Function app $functionAppName already exists."
    }
    else {
        Write-Host "Creating a Function app $functionAppName (Linux, Python 3.11, Consumption) ..."
        $res = az functionapp create --name $functionAppName --resource-group $resourceGroupName `
            --storage-account $functionStorageAccountName --consumption-plan-location $location `
            --os-type Linux --runtime python --runtime-version 3.11 --functions-version 4
        if ($LASTEXITCODE -ne 0) { throw $res }
    }

    # Build requirements.txt remotely (Oryx) during zip deploy.
    az functionapp config appsettings set --name $functionAppName --resource-group $resourceGroupName `
        --settings "SCM_DO_BUILD_DURING_DEPLOYMENT=1" "ENABLE_ORYX_BUILD=true" | Out-Null
}

function Ensure-RoleAssignment([string]$assignee, [string]$role, [string]$scope) {
    $existing = az role assignment list --assignee $assignee --role $role --scope $scope --query "[0].id" --output tsv 2>$null
    if ($existing) {
        Write-Host "Role '$role' already assigned for the scope."
        return
    }
    Create-RoleForScope $assignee $role $scope
}

function Ensure-FunctionIdentityAndRoles {
    Write-Host "Assigning the Function a managed identity and roles ..."
    $principalId = az functionapp identity assign --name $functionAppName --resource-group $resourceGroupName --query principalId --output tsv
    if ([string]::IsNullOrEmpty($principalId)) { throw "Could not assign a managed identity to the Function app." }

    $wsId = az ml workspace show --name $workspaceName --resource-group $resourceGroupName --query id --output tsv
    $stId = az storage account show --name $storageAccountName --resource-group $resourceGroupName --query id --output tsv

    Ensure-RoleAssignment $principalId "Contributor" $wsId
    Ensure-RoleAssignment $principalId "Storage Blob Data Contributor" $stId
}

function Set-RelayAppSettings {
    $settings = @(
        "AML_SUBSCRIPTION_ID=$subscriptionId",
        "AML_RESOURCE_GROUP=$resourceGroupName",
        "AML_WORKSPACE_NAME=$workspaceName",
        "RELAY_DATASTORE=$workspaceBlobDS"
    )

    # The relay is fail-closed: it rejects callers unless the expected tenant + app
    # id are set. Default them to the service principal this script provisions (the
    # same Application id printed for the D365 Demand forecasting parameters page);
    # -d365TenantId / -d365AppId override when D365 authenticates as a different principal.
    $expectedTenant = if ($d365TenantId) { $d365TenantId } else { az account show --query tenantId --output tsv }
    $expectedAppId = if ($d365AppId) { $d365AppId } elseif ($appId) { $appId } elseif ($AADApplicationName) { az ad app list --display-name $AADApplicationName --query "[0].appId" --output tsv }

    if ($expectedTenant -and $expectedAppId) {
        $settings += "RELAY_EXPECTED_TENANT_ID=$expectedTenant"
        $settings += "RELAY_EXPECTED_APP_ID=$expectedAppId"
    }
    else {
        Write-Warning "Could not determine the relay's expected tenant/app id; it will reject D365 calls until RELAY_EXPECTED_TENANT_ID and RELAY_EXPECTED_APP_ID are set (re-run with -d365TenantId and -d365AppId, or -AADApplicationName)."
    }

    az functionapp config appsettings set --name $functionAppName --resource-group $resourceGroupName --settings $settings | Out-Null
}

function Deploy-Relay {
    $relayDir = Join-Path $PSScriptRoot "relay"
    $srcDir = Join-Path $PSScriptRoot "src"
    if (-not (Test-Path $relayDir)) { throw "Relay folder not found next to the script: $relayDir" }

    # The relay imports the pipeline builders from .\src, so ship a copy inside it.
    $relaySrc = Join-Path $relayDir "src"
    if (Test-Path $relaySrc) { Remove-Item $relaySrc -Recurse -Force }
    Copy-Item $srcDir $relaySrc -Recurse
    Get-ChildItem -Path $relayDir -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

    $zipPath = Join-Path $PSScriptRoot "relay_deploy.zip"
    if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
    Compress-Archive -Path (Join-Path $relayDir '*') -DestinationPath $zipPath -Force

    try {
        Write-Host "Deploying the relay code ..."
        # --build-remote true runs the Oryx remote build (pip install requirements.txt)
        # on the Linux host; without it config-zip only extracts files and azure-ai-ml
        # and the other dependencies are missing at runtime.
        $res = az functionapp deployment source config-zip --name $functionAppName --resource-group $resourceGroupName --src $zipPath --build-remote true
        if ($LASTEXITCODE -ne 0) { throw $res }
    }
    finally {
        Remove-Item $zipPath -Force -ErrorAction SilentlyContinue
        Remove-Item $relaySrc -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Ensure-Relay {
    Ensure-FunctionStorage
    Ensure-FunctionApp
    Ensure-FunctionIdentityAndRoles
    Set-RelayAppSettings
    Deploy-Relay
}

function Display-RelayResult {
    Write-Host "`nRelay endpoint - paste into the D365 'Pipeline endpoint address' (MLSPipelineEndpointUri):`n"
    Write-Host (Get-RelayEndpoint)
}

#====

$ErrorActionPreference = "Stop"

#check azure client and extension versions
Check-AzureClientVersion

# login to azure
Write-Host "Please log in with your Azure credentials..."
az login

# check and set the subscription
Set-SubscriptionContext

if ($RelayOnly) {
    # Idempotent relay (re)deploy only; the AML workspace must already exist.
    Ensure-Relay
    Display-RelayResult
    return
}

# check script parameters
Check-InputParams

try
{
    # create a resource group
    Create-ResourceGroup

    # create a storage account
    Create-StorageAccount

    # create ML workspace
    Create-Workspace

    # create workspace compute instance
    Create-ComputeInstance

    # create workspace compute cluster
    Create-ComputeCluster

    # create AAD application for forecast access
    Create-AppWithPrincipal

    # provision and deploy the relay Function (idempotent)
    Ensure-Relay

    # Display script result (includes the relay pipeline endpoint URL)
    Display-ScriptResult
}
catch
{
    Write-Warning "Setup did not complete: $($_.Exception.Message)`nThe script is idempotent - fix the issue and re-run; existing resources are reused (nothing is deleted)."
    throw
}