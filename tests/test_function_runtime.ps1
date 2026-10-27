$ErrorActionPreference = "Stop"

# Load only the function under test; never execute setup/login/deployment.
$tokens = $null
$parseErrors = $null
$source = Join-Path $PSScriptRoot "..\quick_setup.ps1"
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path $source), [ref]$tokens, [ref]$parseErrors
)
if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
$definition = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
    $node.Name -eq "Ensure-FunctionApp"
}, $true)
Invoke-Expression $definition.Extent.Text

$functionAppName = "test-relay"
$resourceGroupName = "test-rg"
$functionStorageAccountName = "teststorage"
$location = "westeurope"

function Assert-True($condition, $message) {
    if (-not $condition) { throw $message }
}

function az {
    $call = $args -join " "
    $script:calls.Add($call)
    $global:LASTEXITCODE = 0
    if ($script:failCommand -and $call.StartsWith($script:failCommand)) {
        $global:LASTEXITCODE = 1
        return ""
    }
    if ($call.StartsWith("functionapp list ")) {
        if ($script:newApp) { return "[]" }
        return ConvertTo-Json -InputObject @($script:app)
    }
    if ($call.StartsWith("functionapp create ")) { return "{}" }
    if ($call.StartsWith("functionapp show ")) { return $script:app | ConvertTo-Json }
    if ($call.StartsWith("appservice plan show ")) {
        return @{ sku = @{ tier = $script:tier; name = $script:sku } } | ConvertTo-Json
    }
    if ($call.StartsWith("functionapp config appsettings list ")) {
        return ConvertTo-Json -InputObject @(
            @{ name = "FUNCTIONS_WORKER_RUNTIME"; value = $script:worker },
            @{ name = "FUNCTIONS_EXTENSION_VERSION"; value = $script:hostVersion }
        )
    }
    if ($call.StartsWith("functionapp config show ")) { return $script:runtime }
    if ($call.StartsWith("functionapp config set ")) {
        if (-not $script:ignoreUpdate) { $script:runtime = "Python|3.12" }
        return "{}"
    }
    if ($call.StartsWith("functionapp config appsettings set ")) { return "[]" }
    throw "Unexpected Azure CLI call: $call"
}

$cases = @(
    @{ name = "new Linux Consumption"; newApp = $true },
    @{ name = "existing 3.12 Consumption" },
    @{ name = "existing Premium upgrade"; tier = "ElasticPremium"; runtime = "Python|3.11"; update = $true },
    @{ name = "existing Dedicated upgrade"; tier = "PremiumV3"; runtime = "Python|3.11"; update = $true },
    @{ name = "old Consumption blocker"; runtime = "Python|3.11"; error = "Cannot upgrade" },
    @{ name = "Flex blocker"; tier = "FlexConsumption"; sku = "FC1"; error = "Flex Consumption" },
    @{ name = "Windows blocker"; windows = $true; error = "Linux Function app" },
    @{ name = "container blocker"; tier = "ElasticPremium"; runtime = "DOCKER|image"; error = "managed Python" },
    @{ name = "unknown plan blocker"; tier = "Unknown"; error = "unknown Function hosting tier" },
    @{ name = "wrong worker blocker"; worker = "node"; error = "Python worker" },
    @{ name = "old host blocker"; hostVersion = "~3"; error = "Functions v4" },
    @{ name = "lookup failure"; failCommand = "functionapp list "; error = "Could not list" },
    @{ name = "creation failure"; newApp = $true; failCommand = "functionapp create "; error = "." },
    @{ name = "plan lookup failure"; failCommand = "appservice plan show "; error = "hosting plan" },
    @{ name = "settings lookup failure"; failCommand = "functionapp config appsettings list "; error = "runtime settings" },
    @{ name = "runtime lookup failure"; failCommand = "functionapp config show "; error = "inspect linuxFxVersion" },
    @{ name = "update failure"; tier = "ElasticPremium"; runtime = "Python|3.11"; failCommand = "functionapp config set "; error = "Could not configure" },
    @{ name = "verification mismatch"; tier = "ElasticPremium"; runtime = "Python|3.11"; ignoreUpdate = $true; error = "verification failed" },
    @{ name = "remote build failure"; failCommand = "functionapp config appsettings set "; error = "remote build settings" }
)

foreach ($case in $cases) {
    $script:calls = [System.Collections.Generic.List[string]]::new()
    $script:newApp = [bool]$case.newApp
    $script:runtime = if ($case.runtime) { $case.runtime } else { "Python|3.12" }
    $script:tier = if ($case.tier) { $case.tier } else { "Dynamic" }
    $script:sku = if ($case.sku) { $case.sku } else { "Y1" }
    $script:worker = if ($case.worker) { $case.worker } else { "python" }
    $script:hostVersion = if ($case.hostVersion) { $case.hostVersion } else { "~4" }
    $script:failCommand = $case.failCommand
    $script:ignoreUpdate = [bool]$case.ignoreUpdate
    $script:app = @{
        name = $functionAppName
        reserved = -not $case.windows
        kind = "functionapp,linux"
        serverFarmId = "/subscriptions/test/resourceGroups/test/providers/Microsoft.Web/serverfarms/test"
    }
    $caught = $null
    try { Ensure-FunctionApp } catch { $caught = $_ }
    if ($case.error) {
        Assert-True ($null -ne $caught) "$($case.name): expected an error"
        Assert-True ($caught.ToString() -match $case.error) "$($case.name): unexpected error: $caught"
    }
    else {
        Assert-True ($null -eq $caught) "$($case.name): unexpected error: $caught"
        if ($case.newApp) {
            Assert-True ([bool]($calls | Where-Object { $_ -match '^functionapp create .*--runtime-version 3.12 ' })) "New app did not request Python 3.12"
        }
        Assert-True (([bool]($calls | Where-Object { $_ -match '^functionapp config set ' })) -eq [bool]$case.update) "$($case.name): incorrect runtime mutation"
        if ($case.update) {
            Assert-True ([bool]($calls | Where-Object { $_ -match '--linux-fx-version Python\|3.12$' })) "Update did not use linuxFxVersion"
        }
    }
    if ($case.name -match 'blocker|lookup failure|creation failure') {
        Assert-True (-not ($calls | Where-Object { $_ -match '^functionapp config (set|appsettings set) ' })) "$($case.name): blocker caused a configuration write"
    }
    Write-Host "PASS: $($case.name)"
}
Write-Host "$($cases.Count) runtime scenarios passed (mocked Azure CLI; no Azure resources touched)."
