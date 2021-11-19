<#
.DESCRIPTION
    This script has the following dependencies:
       * a pre-installed Azure CLI (not the PowerShell one).
       * a pre-installed ML extension azure-cli-ml.
    
    Creates Azure resources required by demand forecast logic for FinOps.
    * Creates a resource group.
    * Creates a storage account
    * Creates and inits an ML workspace:
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
    $res = az storage account create --name $storageAccountName --resource-group $resourceGroupName --location $location --sku $storageSKU --access-tier $storageAccessTier

    if ($LASTEXITCODE -ne 0) {
        # failed creating ML storage account
        Write-Warning "Error while trying to create a storage account."
        throw $res
    }

    $script:storageAccountId = az storage account show --resource-group $resourceGroupName --name $storageAccountName --query "id" --output tsv
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

function Create-Workspace() {
    $WorkSpaceBlobDefaultDS = "workspaceblobdemplan"

    # create ML workspace
    Write-Host "Creating an ML workspace $workspaceName ..."
    $res = az ml workspace create --workspace-name $workspaceName --resource-group $resourceGroupName --storage-account $storageAccountId

    if ($LASTEXITCODE -ne 0) {
        # failed creating ML workspace
        Write-Warning "Error while trying to create an ML workspace."
        throw $res
    }

    $script:workspaceId = az ml workspace show --resource-group $resourceGroupName --workspace-name $workspaceName --query "id" --output tsv

    Write-Host "Setting up an ML workspace datastore ..."
    $res = az ml datastore attach-blob --account-name $storageAccountName --container-name $storageContainer --name $WorkSpaceBlobDefaultDS `
        --account-key $storageAccessKey --workspace-name $workspaceName --resource-group $resourceGroupName `
        --storage-account-resource-group $resourceGroupName --storage-account-subscription-id $subscriptionId

    if ($LASTEXITCODE -ne 0) {
        # failed attaching datastore
        Write-Warning "Error while trying to attach an ML workspace datastore."
        throw $res
    }

    $res = az ml datastore set-default --name $WorkSpaceBlobDefaultDS --resource-group $resourceGroupName --workspace-name $WorkspaceName

    if ($LASTEXITCODE -ne 0) {
        # failed setting default datastore
        Write-Warning "Error while trying to set up a default ML datastore."
        throw $res
    }

	# make sure access keys are in sync
    az ml workspace sync-keys  --workspace-name $WorkspaceName --resource-group $resourceGroupName
}

function Create-ComputeInstance() {
    $computeInstance_Name = "notebookScryptExecutor"
    $computeInstance_vm_size = "Standard_D3_v2"

    # create workspace compute instance
    Write-Host "Creating an ML workspace compute instance ..."
    $res = az ml computetarget create computeinstance --name $computeInstance_Name --resource-group $resourceGroupName --workspace-name $workspaceName --vm-size $computeInstance_vm_size

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
    $res = az ml computetarget create amlcompute --name $computeCluster_Name --resource-group $resourceGroupName --workspace-name $workspaceName `
        --min-nodes $computeCluster_min_nodes --max-nodes $computeCluster_max_nodes --vm-size $computeCluster_vm_size

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
    $appResponse = az ad app create --display-name $AADApplicationName | ConvertFrom-Json

    if ($LASTEXITCODE -ne 0) {
        # failed creating AAD application
        Write-Warning "Error while trying to create an AAD application."
        throw $appResponse
    }

    $script:appId = $appResponse.appId

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

#====

$ErrorActionPreference = "Stop"

$isAzInstalled = Get-Command az -ErrorAction SilentlyContinue
if (-not $isAzInstalled) {
    throw "AZ CLI is not installed, please go get it at: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
}

$mlExtensionName =  az extension list --query "[?name=='azure-cli-ml'].name" --output tsv
if ([string]::IsNullOrEmpty($mlExtensionName)){
    throw "ML extension for AZ CLI azure-cli-ml is not installed. Please install it, see: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
}

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