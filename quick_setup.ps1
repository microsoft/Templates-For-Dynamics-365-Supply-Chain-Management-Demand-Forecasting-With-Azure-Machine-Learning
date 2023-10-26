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

    [Parameter(Mandatory)]
    [string]
    $AADApplicationName
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
    $res = az group exists --resource-group $resourceGroupName
    if ($res -eq "true") {
        Write-Warning "Azure resource group $resourceGroupName already exists and cannot be created."
        throw
    }

    $res = az account list-locations --query "[?name == '$location'].name" --output tsv
    if ([string]::IsNullOrEmpty($res)) {
        Write-Warning "Azure region $location is not valid for the current account."
        throw
    }

    $res = az storage account check-name --name $storageAccountName --query "nameAvailable"
    if ($res -eq "false") {
        Write-Warning "Azure storage account name $storageAccountName is invalid or in use."
        throw
    }
    
    $res = az ad app list --display-name $AADApplicationName --query "[0].appId" --output tsv
    if ($res) {
        Write-Warning "Azure application $AADApplicationName already exists and cannot be created."
        throw
    }

    $res = az ad sp list --filter "displayname eq '$AADApplicationName'" --query "[0].objectId"
    if ($res) {
        Write-Warning "Azure service principal for application $AADApplicationName already exists and cannot be created."
        throw
    }
}

function Create-ResourceGroup() {
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

    # create a storage account
    Write-Host "Creating a storage account $storageAccountName in $location ..."
    $responseObj = az storage account create --name $storageAccountName --resource-group $resourceGroupName --location $location --sku $storageSKU --access-tier $storageAccessTier `
        | ConvertFrom-Json

    if ($LASTEXITCODE -ne 0) {
        # failed creating ML storage account
        Write-Warning "Error while trying to create a storage account."
        throw $responseObj
    }

    $script:storageAccountId = $responseObj.id
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
    # create AAD application for forecast access
    Write-Host "Creating an AAD application for forecast access ..."
    $responseObj = az ad app create --display-name $AADApplicationName | ConvertFrom-Json

    if ($LASTEXITCODE -ne 0) {
        # failed creating AAD application
        Write-Warning "Error while trying to create an AAD application."
        throw $responseObj
    }

    $script:appId = $responseObj.appId

    # create service principal
    Write-Host "Creating a service principal for an application $AADApplicationName ..."
    $res = az ad sp create --id $appId

    if ($LASTEXITCODE -ne 0) {
        # failed creating service principal
        Write-Warning "Error while trying to create a service principal."
        throw $res
    }

    # create security roles
    Create-RoleForScope $appId "Contributor" $workspaceId
    Create-RoleForScope $appId "Contributor" $storageAccountId
    Create-RoleForScope $appId "Storage Blob Data Contributor" $storageAccountId
}

function Display-ScriptResult{
    $azureTenantId = az account show --query "tenantId" --output tsv
    $documentationLink = "https://go.microsoft.com/fwlink/?linkid=2165514"

    Write-Host "`nDemand forecast setup script has completed."
    Write-Host "Please create an application secret explicitly and proceed with workspace pipeline configuration according to public documentation: $documentationLink"
    
    # display information needed by FinOps for AML demand forecast
    Write-Host "`nDemand forecast parameters.`n"
    Write-Host "Azure tenant id: $azureTenantId"
    Write-Host "Storage account name: $storageAccountName"
    Write-Host "Application id: $appId"
}

function Do-ResourcesCleanUp {
    #delete already created resources.

    Write-Host "Cleaning up ..."
    $resourceExists = az group exists --resource-group $resourceGroupName
    if ($resourceExists -eq "true") {
        az group delete --resource-group $resourceGroupName --no-wait --yes
    }

    $appId = az ad app list --display-name $AADApplicationName --query "[0].appId" --output tsv
    if ($appId) {
        az ad app delete --id $appId
    }

    $appId = az ad sp list --filter "displayname eq '$AADApplicationName'" --query "[0].appId"
    if ($appId) {
        az ad sp delete --id $appId
    }
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

#====

$ErrorActionPreference = "Stop"

#check azure client and extension versions
Check-AzureClientVersion

# login to azure
Write-Host "Please log in with your Azure credentials..."
az login

# check and set the subscription
Set-SubscriptionContext

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

    # Display script result
    Display-ScriptResult
}
catch
{
    Write-Warning "Error happened during script execution. The setup did not complete.`nPlease fix the issue, delete all created resources, and re-run the script."

    Do-ResourcesCleanUp
}