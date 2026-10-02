#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Uninstalls the Marabunta Compute Swarm Node Windows Service.

.DESCRIPTION
    Stops the service if running, removes the service registration, removes
    the firewall rule, and optionally removes data directories.

.PARAMETER ServiceName
    Windows Service name (default: MarabuntaSwarm).

.PARAMETER RemoveData
    If specified, removes configuration, data, and log directories under ProgramData.

.PARAMETER RemoveBinary
    If specified, removes the installed binary from Program Files.

.EXAMPLE
    .\uninstall-windows-service.ps1

.EXAMPLE
    .\uninstall-windows-service.ps1 -RemoveData -RemoveBinary
#>

param(
    [string]$ServiceName = "MarabuntaSwarm",
    [switch]$RemoveData,
    [switch]$RemoveBinary
)

$ErrorActionPreference = "Stop"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Marabunta Compute Swarm Node - Service Uninstaller" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# -------------------------------------------------------------------------
# 1. Stop the service if it is running
# -------------------------------------------------------------------------
$Svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($Svc) {
    if ($Svc.Status -eq "Running") {
        Write-Host "[1/4] Stopping service '$ServiceName'..."
        Stop-Service -Name $ServiceName -Force
        Start-Sleep -Seconds 3
        Write-Host "  Service stopped." -ForegroundColor Green
    } else {
        Write-Host "[1/4] Service '$ServiceName' is not running (status: $($Svc.Status))" -ForegroundColor Yellow
    }
} else {
    Write-Host "[1/4] Service '$ServiceName' is not installed." -ForegroundColor Yellow
    Write-Host "  Nothing to uninstall."
    exit 0
}

# -------------------------------------------------------------------------
# 2. Delete the service
# -------------------------------------------------------------------------
Write-Host "[2/4] Removing service registration..."
sc.exe delete $ServiceName | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  Service deleted." -ForegroundColor Green
} else {
    Write-Warning "  sc.exe delete returned exit code $LASTEXITCODE. The service may require a reboot to fully remove."
}

# -------------------------------------------------------------------------
# 3. Remove firewall rule
# -------------------------------------------------------------------------
$FwRuleName = "MarabuntaSwarm"
$ExistingRule = Get-NetFirewallRule -Name $FwRuleName -ErrorAction SilentlyContinue
if ($ExistingRule) {
    Remove-NetFirewallRule -Name $FwRuleName
    Write-Host "[3/4] Firewall rule '$FwRuleName' removed." -ForegroundColor Green
} else {
    Write-Host "[3/4] Firewall rule '$FwRuleName' not found (skipped)." -ForegroundColor Yellow
}

# -------------------------------------------------------------------------
# 4. Optionally remove data, config, logs, and binary
# -------------------------------------------------------------------------
$DataRoot = "$env:ProgramData\MarabuntaCompute"
$BinaryPath = "$env:ProgramFiles\MarabuntaCompute"

if ($RemoveData) {
    if (Test-Path $DataRoot) {
        Remove-Item -Path $DataRoot -Recurse -Force
        Write-Host "[4/4] Data directory removed: $DataRoot" -ForegroundColor Green
    } else {
        Write-Host "[4/4] Data directory not found: $DataRoot (skipped)" -ForegroundColor Yellow
    }
} else {
    Write-Host "[4/4] Data directory preserved: $DataRoot" -ForegroundColor Yellow
    Write-Host "  Use -RemoveData to delete configuration, data, and logs."
}

if ($RemoveBinary) {
    if (Test-Path $BinaryPath) {
        Remove-Item -Path $BinaryPath -Recurse -Force
        Write-Host "  Binary directory removed: $BinaryPath" -ForegroundColor Green
    }
}

# Remove registry keys.
$RegPath = "HKLM:\SOFTWARE\MarabuntaCompute"
if (Test-Path $RegPath) {
    Remove-Item -Path $RegPath -Recurse -Force
    Write-Host "  Registry keys removed: $RegPath" -ForegroundColor Green
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Uninstallation Complete" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
