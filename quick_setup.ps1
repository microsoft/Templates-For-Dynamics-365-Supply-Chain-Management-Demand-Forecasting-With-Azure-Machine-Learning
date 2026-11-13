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

    # A display name is a creation label, never an identity to reuse.
    [Parameter(ParameterSetName = 'Full')]
    [ValidateNotNullOrEmpty()]
    [string]
    $AADApplicationName,

    [Parameter(ParameterSetName = 'Full')]
    [ValidateNotNullOrEmpty()]
    [string]
    $AADApplicationId = "",

    # Relay (Azure Function) that bridges D365's v1 pipeline-endpoint contract to v2.
    [Parameter(Mandatory)]
    [string]
    $functionAppName,

    [Parameter(Mandatory)]
    [string]
    $functionStorageAccountName,

    # Optional overrides for the relay's pinned D365 caller (tenant + client ID).
    [ValidateNotNullOrEmpty()]
    [string]
    $d365TenantId = "",

    [ValidateNotNullOrEmpty()]
    [string]
    $d365AppId = "",

    # Optional exact audience override; otherwise discover using a secure secret prompt.
    [ValidateNotNullOrEmpty()]
    [string]
    $d365TokenAudience = "",

    # Re-deploy only the relay (idempotent); skips the AML workspace setup.
    [Parameter(ParameterSetName = 'RelayOnly')]
    [switch]
    $RelayOnly
)

$storageContainer = "demplan-azureml"  #should not be changed
$computeCluster_Name = "e2ecpucluster" #should not be changed
$workspaceBlobDS = "workspaceblobdemplan"
$script:azureTenantId = $null
$script:applicationClientId = $null
$script:applicationObjectId = $null
$script:applicationCreated = $false
$script:servicePrincipalObjectId = $null
$script:relayExpectedTenantId = $null
$script:relayExpectedAppId = $null
$script:relayExpectedAudience = $null

function ConvertTo-IdentityGuid([string]$value, [string]$label) {
    $parsed = [guid]::Empty
    if (-not [guid]::TryParseExact($value, "D", [ref]$parsed) -or $parsed -eq [guid]::Empty) {
        throw "$label must be a non-empty GUID in xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx format."
    }
    return $parsed.ToString()
}

function Assert-TokenAudience([string]$value) {
    $guidAudience = [guid]::Empty
    $uriAudience = $null
    $isGuid = [guid]::TryParseExact($value, "D", [ref]$guidAudience) -and $guidAudience -ne [guid]::Empty
    $isUri = [uri]::TryCreate($value, [UriKind]::Absolute, [ref]$uriAudience) -and
        $uriAudience.Scheme -in @("https", "api") -and $uriAudience.Host -and
        -not $uriAudience.UserInfo -and -not $uriAudience.Query -and -not $uriAudience.Fragment
    if ([string]::IsNullOrWhiteSpace($value) -or $value -match '\s|[\x00-\x1f\x7f*<>{}\[\],;|@"''\\?#]' -or
        $value -match '(^|/)\.default(/|$)' -or -not ($isGuid -or $isUri)) {
        throw "Specify -d365TokenAudience as one exact operator-verified legacy token aud (resource URI or resource GUID), not a scope, /.default value, caller client ID, or guessed Function URL."
    }
}

function Assert-SetupInteractive([string]$guidance) {
    $nonInteractive = @([Environment]::GetCommandLineArgs() | Where-Object { $_ -like "-noni*" }).Count -gt 0
    if (-not [Environment]::UserInteractive -or $nonInteractive -or
        ($Host.Name -eq "ConsoleHost" -and [Console]::IsInputRedirected)) {
        throw $guidance
    }
}

function Assert-AudienceDiscoveryInteractive {
    Assert-SetupInteractive "Audience discovery requires an interactive secure prompt. Supply a verified -d365TokenAudience when running unattended or with redirected input."
}

function Read-SetupValue([string]$prompt, [string]$parameterGuidance) {
    Assert-SetupInteractive "Setup requires interactive input. $parameterGuidance when running unattended or with redirected input."
    try {
        $value = Read-Host $prompt -ErrorAction Stop
    }
    catch {
        throw "Setup input was canceled or unavailable. $parameterGuidance to run without this prompt."
    }
    if ($value -isnot [string] -or [string]::IsNullOrWhiteSpace($value)) {
        throw "Setup input cannot be empty. $parameterGuidance to provide it explicitly."
    }
    return $value
}

function Read-SetupIdentityInputs {
    if ($RelayOnly) {
        if (-not $d365AppId) {
            $script:d365AppId = Read-SetupValue "D365 application client ID (GUID, not an object ID)" "Supply -d365AppId"
        }
    }
    elseif (-not $AADApplicationId -and -not $AADApplicationName) {
        Write-Host "Forecast application:"
        Write-Host "1. Create a new Entra application"
        Write-Host "2. Use an existing Entra application (client ID)"
        $choice = Read-SetupValue "Select 1 or 2" "Supply -AADApplicationName or -AADApplicationId"
        switch ($choice.Trim()) {
            "1" {
                $script:AADApplicationName = Read-SetupValue "New forecast application display name" "Supply -AADApplicationName"
            }
            "2" {
                $script:AADApplicationId = Read-SetupValue "Approved forecast application client ID (GUID, not a display name or object ID)" "Supply -AADApplicationId"
            }
            default {
                throw "Select 1 or 2. No application was selected. Alternatively, supply -AADApplicationName or -AADApplicationId."
            }
        }
    }
}

function ConvertFrom-TokenSegment([string]$segment) {
    $bytes = $parseOptions = $result = $null
    try {
        $segment = $segment.Replace("-", "+").Replace("_", "/")
        switch ($segment.Length % 4) {
            0 { }
            2 { $segment += "==" }
            3 { $segment += "=" }
            default { throw "Invalid JWT segment encoding." }
        }
        $bytes = [Convert]::FromBase64String($segment)
        $parseOptions = @{
            InputObject = ([Text.UTF8Encoding]::new($false, $true)).GetString($bytes)
            ErrorAction = "Stop"
        }
        if ((Get-Command ConvertFrom-Json).Parameters.ContainsKey("NoEnumerate")) {
            $parseOptions.NoEnumerate = $true
        }
        $result = ConvertFrom-Json @parseOptions
        if ($result -is [array] -or $result -isnot [pscustomobject]) { throw "A JWT segment must contain a JSON object." }
        return $result
    }
    finally {
        if ($bytes) { [Array]::Clear($bytes, 0, $bytes.Length) }
        Remove-Variable segment, bytes, parseOptions, result -ErrorAction SilentlyContinue
    }
}

function Get-D365TokenAudience([string]$tenantId, [string]$clientId) {
    $tenantId = ConvertTo-IdentityGuid $tenantId "D365 caller tenant ID"
    $clientId = ConvertTo-IdentityGuid $clientId "D365 caller application client ID"
    Assert-AudienceDiscoveryInteractive
    Write-Host "Discovering the token audience for D365 caller $clientId in tenant $tenantId."
    if ($script:applicationCreated -and $clientId -eq $script:applicationClientId -and $tenantId -eq $script:azureTenantId) {
        Write-Host "Create the client secret in Azure portal while this script waits:"
        Write-Host "1. Open https://portal.azure.com. In Settings > Directories + subscriptions, switch to tenant $tenantId."
        Write-Host "2. Use the top search to open App registrations > All applications. Search for application (client) ID $clientId and select the matching app."
        Write-Host "3. Open Certificates & secrets > Client secrets > New client secret."
        Write-Host "4. Enter a description and choose an expiry according to your organization's policy, then select Add."
        Write-Host "5. Copy the secret VALUE (not Secret ID), retain it securely for D365, and enter it at the secure prompt below. Setup will then continue."
    }
    else {
        Write-Host "Use the active client secret for this D365 application. Previously created secret Values cannot be retrieved from Entra; coordinate with the D365 administrator if needed."
    }
    Write-Host "The secret and token are used only in memory and are not printed or saved. Do not enable HTTP tracing or request logging."

    $secret = $credential = $body = $response = $parts = $header = $claims = $null
    $stage = "prompt"
    try {
        $secret = Read-Host "D365 application client secret" -AsSecureString
        if ($secret -isnot [Security.SecureString] -or $secret.Length -eq 0) {
            throw "A client secret is required."
        }
        $stage = "request"
        $credential = [Net.NetworkCredential]::new("", $secret)
        $body = @{
            client_id = $clientId
            client_secret = $credential.Password
            scope = "https://management.azure.com/.default"
            grant_type = "client_credentials"
        }
        $response = Invoke-RestMethod -Method Post `
            -Uri "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token" `
            -ContentType "application/x-www-form-urlencoded" -Body $body `
            -TimeoutSec 30 -MaximumRedirection 0 -ErrorAction Stop -Verbose:$false -Debug:$false
        $body.Clear()
        $credential.Password = ""
        $secret.Dispose()
        $secret = $null

        # Inspect only the direct Entra HTTPS response; the relay still verifies signatures.
        $stage = "response"
        if ($response -is [array] -or $response -isnot [pscustomobject] -or $response.token_type -isnot [string] -or
            $response.token_type -ne "Bearer" -or $response.access_token -isnot [string] -or
            $response.access_token -cnotmatch '^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$') {
            throw "The token endpoint did not return a bearer JWT."
        }
        $parts = $response.access_token.Split(".")
        $header = ConvertFrom-TokenSegment $parts[0]
        $claims = ConvertFrom-TokenSegment $parts[1]
        if ($header.alg -isnot [string] -or $header.alg -cne "RS256" -or
            $header.kid -isnot [string] -or [string]::IsNullOrWhiteSpace($header.kid)) {
            throw "The token does not use a supported signing profile."
        }
        if ($claims.ver -isnot [string]) { throw "The token version must be a string." }
        if ($claims.ver -ceq "1.0") {
            $issuer = "https://sts.windows.net/$tenantId/"
            $callerClaim = "appid"
            $otherCallerClaim = "azp"
        }
        elseif ($claims.ver -ceq "2.0") {
            $issuer = "https://login.microsoftonline.com/$tenantId/v2.0"
            $callerClaim = "azp"
            $otherCallerClaim = "appid"
        }
        else { throw "The token has an unsupported version." }
        if ($claims.iss -isnot [string] -or $claims.iss -cne $issuer -or
            $claims.tid -isnot [string] -or $claims.$callerClaim -isnot [string] -or
            (ConvertTo-IdentityGuid $claims.tid "Token tenant ID") -ne $tenantId -or
            (ConvertTo-IdentityGuid $claims.$callerClaim "Token caller client ID") -ne $clientId) {
            throw "The token does not identify the expected tenant and application."
        }
        if ($claims.PSObject.Properties[$otherCallerClaim] -and
            ($claims.$otherCallerClaim -isnot [string] -or
                (ConvertTo-IdentityGuid $claims.$otherCallerClaim "Additional token caller ID") -ne $clientId)) {
            throw "The token has contradictory caller claims."
        }
        $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
        if ($null -eq $claims.PSObject.Properties["exp"]) { throw "The token has no expiration." }
        foreach ($name in @("exp", "nbf", "iat")) {
            if ($claims.PSObject.Properties[$name]) {
                $date = $claims.$name
                if (-not ($date -is [int] -or $date -is [long] -or $date -is [double] -or $date -is [decimal]) -or
                    [double]::IsNaN([double]$date) -or [double]::IsInfinity([double]$date) -or
                    ($name -eq "exp" -and $date -le $now) -or ($name -ne "exp" -and $date -gt $now)) {
                    throw "The token has invalid lifetime claims."
                }
            }
        }
        if ($claims.aud -isnot [string]) { throw "The token must contain one string audience." }
        Assert-TokenAudience $claims.aud
        return $claims.aud
    }
    catch {
        if ($stage -eq "prompt") {
            throw "Audience discovery needs the D365 application's client secret. Enter it at the secure prompt, or supply a verified -d365TokenAudience to skip discovery."
        }
        if ($stage -eq "request") {
            throw "Could not request the D365 application token. Check its tenant, active secret, directory propagation and HTTPS access to Microsoft Entra, or supply a verified -d365TokenAudience. No token response is displayed."
        }
        throw "Could not determine an audience from the token response: a supported, current bearer JWT for the selected tenant/application with one resource audience is required. Check the application configuration and system clock, or supply a verified -d365TokenAudience."
    }
    finally {
        if ($body) { $body.Clear() }
        if ($response -isnot [array] -and $response -is [pscustomobject] -and
            $response.PSObject.Properties["access_token"]) { $response.access_token = $null }
        if ($credential) { $credential.Password = "" }
        if ($secret -is [Security.SecureString]) { $secret.Dispose() }
        if ($parts) { [Array]::Clear($parts, 0, $parts.Length) }
        Remove-Variable secret, credential, body, response, parts, header, claims -ErrorAction SilentlyContinue
    }
}

function Invoke-IdentityAzJson([string[]]$arguments) {
    $json = az @arguments --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI failed during identity setup: $($arguments[0]) $($arguments[1])."
    }
    if ([string]::IsNullOrWhiteSpace(($json -join "`n"))) {
        throw "Azure CLI returned no JSON during identity setup: $($arguments[0]) $($arguments[1])."
    }
    try {
        $parseOptions = @{ InputObject = ($json -join "`n"); ErrorAction = "Stop" }
        # PowerShell 7 otherwise unwraps singleton arrays and drops empty arrays.
        if ((Get-Command ConvertFrom-Json).Parameters.ContainsKey("NoEnumerate")) {
            $parseOptions.NoEnumerate = $true
        }
        $result = ConvertFrom-Json @parseOptions
    }
    catch {
        throw "Azure CLI returned invalid JSON during identity setup: $($arguments[0]) $($arguments[1])."
    }
    if ($null -eq $result) {
        throw "Azure CLI returned null during identity setup: $($arguments[0]) $($arguments[1])."
    }
    return ,$result
}

function Check-IdentityInputs {
    $script:subscriptionId = ConvertTo-IdentityGuid $subscriptionId "Subscription ID"
    if ($AADApplicationId) {
        $script:AADApplicationId = ConvertTo-IdentityGuid $AADApplicationId "Forecast application client ID"
    }
    if ($d365AppId) {
        $script:d365AppId = ConvertTo-IdentityGuid $d365AppId "D365 caller application client ID"
    }
    if ($d365TenantId) {
        $script:d365TenantId = ConvertTo-IdentityGuid $d365TenantId "D365 caller tenant ID"
    }
    if ($RelayOnly) {
        if (-not $d365AppId) {
            throw "Relay-only setup requires an operator-approved -d365AppId (application client ID); display names are not accepted."
        }
    }
    elseif (-not $AADApplicationId -and [string]::IsNullOrWhiteSpace($AADApplicationName)) {
        throw "Specify -AADApplicationId to reuse an operator-approved forecast application, or -AADApplicationName to create a new application."
    }
    if ($d365TokenAudience) { Assert-TokenAudience $d365TokenAudience }
    else { Assert-AudienceDiscoveryInteractive }
}

function Assert-SubscriptionContext {
    $account = Invoke-IdentityAzJson @("account", "show")
    if ($account -is [array] -or $account -isnot [pscustomobject]) { throw "Azure CLI returned an invalid subscription context." }
    $actualSubscription = ConvertTo-IdentityGuid $account.id "Active subscription ID"
    $actualTenant = ConvertTo-IdentityGuid $account.tenantId "Active subscription tenant ID"
    if ($actualSubscription -ne $subscriptionId) {
        throw "The active subscription does not match -subscriptionId."
    }
    if ($script:azureTenantId -and $actualTenant -ne $script:azureTenantId) {
        throw "The active directory tenant changed during setup."
    }
    $script:azureTenantId = $actualTenant
}

function Set-SubscriptionContext {
    az account set --subscription $subscriptionId
    if ($LASTEXITCODE -ne 0) { throw "Could not select the requested Azure subscription." }
    Assert-SubscriptionContext
}

function Resolve-RelayCaller {
    $tenant = if ($d365TenantId) { $d365TenantId } else { $script:azureTenantId }
    $caller = if ($d365AppId) { $d365AppId } elseif (-not $RelayOnly) { $script:applicationClientId }
    $script:relayExpectedTenantId = ConvertTo-IdentityGuid $tenant "Relay caller tenant ID"
    $script:relayExpectedAppId = ConvertTo-IdentityGuid $caller "Relay caller application client ID"
    $script:relayExpectedAudience = $null
    if ($d365TokenAudience) {
        Assert-TokenAudience $d365TokenAudience
        $script:relayExpectedAudience = $d365TokenAudience
    }
    else {
        $script:relayExpectedAudience = Get-D365TokenAudience $script:relayExpectedTenantId $script:relayExpectedAppId
    }
    Assert-RelayCaller
    Write-Host "D365 caller tenant: $script:relayExpectedTenantId; application client ID: $script:relayExpectedAppId"
    Write-Host "Token audience: $script:relayExpectedAudience"
}

function Assert-RelayCaller {
    Assert-TokenAudience $script:relayExpectedAudience
    $script:relayExpectedTenantId = ConvertTo-IdentityGuid $script:relayExpectedTenantId "Relay caller tenant ID"
    $script:relayExpectedAppId = ConvertTo-IdentityGuid $script:relayExpectedAppId "Relay caller application client ID"
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

function Create-RoleForScope([string]$principalObjectId, [string]$role, [string]$scope) {
    $assignment = Invoke-IdentityAzJson @("role", "assignment", "create",
        "--assignee-object-id", $principalObjectId, "--assignee-principal-type", "ServicePrincipal",
        "--role", $role, "--scope", $scope, "--subscription", $subscriptionId)
    if ($assignment -is [array] -or $assignment -isnot [pscustomobject]) { throw "Azure CLI returned an invalid role assignment." }
    if ((ConvertTo-IdentityGuid $assignment.principalId "Assigned principal object ID") -ne $principalObjectId -or
        $assignment.principalType -ne "ServicePrincipal" -or $assignment.scope -ne $scope) {
        throw "Azure CLI returned an unexpected role assignment principal or scope."
    }
}

function Create-AppWithPrincipal {
    $script:applicationCreated = $false
    if ($AADApplicationId) {
        $application = Invoke-IdentityAzJson @("ad", "app", "show", "--id", $AADApplicationId,
            "--query", "{appId:appId,id:id}")
        if ($application -is [array] -or $application -isnot [pscustomobject]) { throw "Azure CLI returned an invalid application." }
        $resolvedClientId = ConvertTo-IdentityGuid $application.appId "Resolved application client ID"
        # `az ad app show --id` also accepts an application object ID.
        if ($resolvedClientId -ne $AADApplicationId) {
            throw "-AADApplicationId must be the intended application client ID, not an application object ID."
        }
    }
    else {
        $candidates = Invoke-IdentityAzJson @("ad", "app", "list", "--display-name", $AADApplicationName,
            "--all", "--query", "[].{appId:appId,id:id}")
        if ($candidates -isnot [array]) {
            throw "Azure CLI returned an invalid application list."
        }
        if ($candidates.Count -ne 0) {
            throw "Applications already match '$AADApplicationName'. Verify the intended application with your administrator and rerun with its client ID in -AADApplicationId. No application was selected by name."
        }
        Write-Host "Creating an AAD application for forecast access ..."
        $application = Invoke-IdentityAzJson @("ad", "app", "create", "--display-name", $AADApplicationName,
            "--query", "{appId:appId,id:id}")
        if ($application -is [array] -or $application -isnot [pscustomobject]) { throw "Azure CLI returned an invalid created application." }
        $resolvedClientId = ConvertTo-IdentityGuid $application.appId "Created application client ID"
        $script:applicationCreated = $true
    }

    $script:applicationClientId = $resolvedClientId
    Write-Host "Forecast application client ID: $applicationClientId. For reruns, use -AADApplicationId $applicationClientId."
    $script:applicationObjectId = ConvertTo-IdentityGuid $application.id "Application object ID"

    $principals = Invoke-IdentityAzJson @("ad", "sp", "list", "--filter", "appId eq '$applicationClientId'",
        "--all", "--query", "[].{id:id,appId:appId,appOwnerOrganizationId:appOwnerOrganizationId,servicePrincipalType:servicePrincipalType}")
    if ($principals -isnot [array] -or $principals.Count -gt 1) {
        throw "Azure CLI did not return a unique service principal for the pinned application."
    }
    if ($principals.Count -eq 1) {
        $principal = $principals[0]
    }
    else {
        $principal = Invoke-IdentityAzJson @("ad", "sp", "create", "--id", $applicationClientId)
    }
    if ($principal -is [array] -or $principal -isnot [pscustomobject]) { throw "Azure CLI returned an invalid service principal." }
    $principalClientId = ConvertTo-IdentityGuid $principal.appId "Service principal application client ID"
    $ownerTenantId = ConvertTo-IdentityGuid $principal.appOwnerOrganizationId "Application owner tenant ID"
    if ($principalClientId -ne $applicationClientId -or $ownerTenantId -ne $script:azureTenantId -or
        $principal.servicePrincipalType -ne "Application") {
        throw "The service principal does not map to the pinned application in the selected subscription's tenant."
    }
    $script:servicePrincipalObjectId = ConvertTo-IdentityGuid $principal.id "Service principal object ID"
}

function Grant-ForecastAppRoles {
    Assert-SubscriptionContext
    Ensure-RoleAssignment $script:servicePrincipalObjectId "Contributor" $workspaceId
    Ensure-RoleAssignment $script:servicePrincipalObjectId "Contributor" $storageAccountId
    Ensure-RoleAssignment $script:servicePrincipalObjectId "Storage Blob Data Contributor" $storageAccountId
}

function Get-RelayEndpoint {
    # Only the authority of this URL is used by D365; the path segments are parsed
    # for subscription / resource group / workspace / Id, so they must all be present.
    $fqdn = az functionapp show --name $functionAppName --resource-group $resourceGroupName --query defaultHostName --output tsv
    return "https://$fqdn/pipelines/v1.0/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.MachineLearningServices/workspaces/$workspaceName/PipelineRuns/PipelineEndpointSubmit/Id/relay"
}

function Display-ScriptResult{
    $documentationLink = "https://go.microsoft.com/fwlink/?linkid=2165514"

    Write-Host "`nDemand forecast setup script has completed."
    Write-Host "Use the D365 application's client secret (the one supplied during audience discovery, if used) for the Demand forecasting parameters. Follow: $documentationLink"
    
    # display information needed by FinOps for AML demand forecast
    Write-Host "`nDemand forecast parameters (enter these on the D365 Demand forecasting parameters page).`n"
    Write-Host "Azure tenant id: $script:relayExpectedTenantId"
    Write-Host "Storage account name: $storageAccountName"
    Write-Host "Application id: $script:relayExpectedAppId"
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
        Write-Host "Creating a Function app $functionAppName (Linux, Python 3.12, Consumption) ..."
        $res = az functionapp create --name $functionAppName --resource-group $resourceGroupName `
            --storage-account $functionStorageAccountName --consumption-plan-location $location `
            --os-type Linux --runtime python --runtime-version 3.12 --functions-version 4
        if ($LASTEXITCODE -ne 0) { throw $res }
    }

    # Build requirements.txt remotely (Oryx) during zip deploy.
    az functionapp config appsettings set --name $functionAppName --resource-group $resourceGroupName `
        --settings "SCM_DO_BUILD_DURING_DEPLOYMENT=1" "ENABLE_ORYX_BUILD=true" | Out-Null
}

function Ensure-RoleAssignment([string]$principalObjectId, [string]$role, [string]$scope) {
    $principalObjectId = ConvertTo-IdentityGuid $principalObjectId "Role assignee service principal object ID"
    if ([string]::IsNullOrWhiteSpace($scope)) { throw "A role assignment requires an explicit resource scope." }
    $assignments = Invoke-IdentityAzJson @("role", "assignment", "list", "--assignee-object-id", $principalObjectId,
        "--scope", $scope, "--fill-principal-name", "false", "--subscription", $subscriptionId,
        "--query", "[].{principalId:principalId,roleDefinitionName:roleDefinitionName,scope:scope}")
    if ($assignments -isnot [array]) { throw "Azure CLI returned an invalid role assignment list." }
    foreach ($assignment in $assignments) {
        if ($assignment -is [array] -or $assignment -isnot [pscustomobject]) { throw "Azure CLI returned an invalid role assignment entry." }
        $assignedPrincipal = ConvertTo-IdentityGuid $assignment.principalId "Existing role assignee object ID"
        if ([string]::IsNullOrWhiteSpace($assignment.scope) -or [string]::IsNullOrWhiteSpace($assignment.roleDefinitionName)) {
            throw "Azure CLI returned an incomplete role assignment."
        }
        if ($assignedPrincipal -eq $principalObjectId -and $assignment.roleDefinitionName -eq $role -and $assignment.scope -eq $scope) {
            Write-Host "Role '$role' already assigned to the verified principal for the exact scope."
            return
        }
    }
    Create-RoleForScope $principalObjectId $role $scope
}

function Ensure-FunctionIdentityAndRoles {
    Assert-SubscriptionContext
    Write-Host "Assigning the Function a managed identity and roles ..."
    $identity = Invoke-IdentityAzJson @("functionapp", "identity", "assign", "--name", $functionAppName, "--resource-group", $resourceGroupName)
    if ($identity -is [array] -or $identity -isnot [pscustomobject]) { throw "Azure CLI returned an invalid Function managed identity." }
    $principalId = ConvertTo-IdentityGuid $identity.principalId "Function managed identity principal ID"

    $wsId = az ml workspace show --name $workspaceName --resource-group $resourceGroupName --query id --output tsv
    if ($LASTEXITCODE -ne 0) { throw "Could not resolve the Function's workspace role scope." }
    $stId = az storage account show --name $storageAccountName --resource-group $resourceGroupName --query id --output tsv
    if ($LASTEXITCODE -ne 0) { throw "Could not resolve the Function's storage role scope." }

    Ensure-RoleAssignment $principalId "Contributor" $wsId
    Ensure-RoleAssignment $principalId "Storage Blob Data Contributor" $stId
}

function Set-RelayAppSettings {
    Assert-RelayCaller
    $settings = @(
        "AML_SUBSCRIPTION_ID=$subscriptionId",
        "AML_RESOURCE_GROUP=$resourceGroupName",
        "AML_WORKSPACE_NAME=$workspaceName",
        "RELAY_DATASTORE=$workspaceBlobDS",
        "RELAY_EXPECTED_TENANT_ID=$script:relayExpectedTenantId",
        "RELAY_EXPECTED_APP_ID=$script:relayExpectedAppId",
        "RELAY_EXPECTED_AUDIENCE=$script:relayExpectedAudience"
    )

    az functionapp config appsettings set --name $functionAppName --resource-group $resourceGroupName --settings $settings | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Could not configure the relay's pinned caller identity; deployment stopped." }
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
    Assert-RelayCaller
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

Read-SetupIdentityInputs
Check-IdentityInputs

#check azure client and extension versions
Check-AzureClientVersion

# login to azure
Write-Host "Please log in with your Azure credentials..."
az login
if ($LASTEXITCODE -ne 0) { throw "Azure login failed." }

# check and set the subscription
Set-SubscriptionContext

try
{
    if (-not $RelayOnly) {
        Check-InputParams
        # Resolve the forecast identity before provisioning resources or granting roles.
        Create-AppWithPrincipal
    }
    Resolve-RelayCaller

    if ($RelayOnly) {
        # The AML workspace must already exist.
        Ensure-Relay
        Display-RelayResult
        return
    }

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

    # grant the verified service principal access to the intended resources
    Grant-ForecastAppRoles

    # provision and deploy the relay Function (idempotent)
    Ensure-Relay

    # Display script result (includes the relay pipeline endpoint URL)
    Display-ScriptResult
}
catch
{
    Write-Warning "Setup did not complete: $($_.Exception.Message)`nFix the issue and rerun; if an application was created, reuse its printed client ID with -AADApplicationId. Existing Azure resources are reused (nothing is deleted)."
    throw
}