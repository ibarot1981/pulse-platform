param(
    [switch]$KillExisting
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$botPattern = 'pulse(\\|/)main\.py|pulse\.main'
$existing = Get-CimInstance Win32_Process |
    Where-Object { $_.Name -match 'python' -and $_.CommandLine -match $botPattern }

if ($existing) {
    if (-not $KillExisting) {
        Write-Host "Another bot instance is running. Use -KillExisting to terminate it first."
        $existing | Select-Object ProcessId, Name, CommandLine | Format-Table -AutoSize
        exit 1
    }

    $pids = $existing | Select-Object -ExpandProperty ProcessId
    Stop-Process -Id $pids -Force
    Start-Sleep -Seconds 1
}

Write-Host "Starting bot: python -m pulse.main"
python -m pulse.main
