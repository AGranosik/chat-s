param(
  [int[]]  $Rooms        = @(1, 10, 100, 1000),
  [int[]]  $Users        = @(2, 5, 10),
  [int]    $Ramp         = 150,
  [int]    $Settle       = 5,
  [int]    $Duration     = 30,
  [int]    $Drain        = 30,
  [int]    $SendInterval = 20,
  [string] $HttpBase     = "http://localhost:80"
)

$ErrorActionPreference = "Stop"
$script = Join-Path $PSScriptRoot "chat_load.js"
$outDir = Join-Path $PSScriptRoot "results\matrix"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

if (-not (Get-Command k6 -ErrorAction SilentlyContinue)) {
  throw "k6 not found on PATH. Install from https://k6.io/docs/get-started/installation/"
}

$failed = @()
foreach ($roomCount in $Rooms) {
  foreach ($userCount in $Users) {
    $name = "rooms{0}_users{1}" -f $roomCount, $userCount
    $vus = $roomCount * $userCount
    Write-Host "=== $name (VUs=$vus, ramp=${Ramp}s, hold=${Duration}s) ===" -ForegroundColor Cyan

    $env:ROOMS = "$roomCount"
    $env:USERS = "$userCount"
    $env:RAMP = "$Ramp"
    $env:SETTLE = "$Settle"
    $env:DURATION = "$Duration"
    $env:DRAIN = "$Drain"
    $env:SEND_INTERVAL = "$SendInterval"
    $env:MODE = "tput"
    $env:HTTP_BASE = $HttpBase
    $summary = Join-Path $outDir "$name.json"

    k6 run --summary-export $summary $script
    if ($LASTEXITCODE -ne 0) {
      Write-Warning "$name breached a threshold; summary: $summary"
      $failed += $name
    }
  }
}

Write-Host "Summaries: $outDir" -ForegroundColor Green
Write-Host "=== Load-test summary ===" -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "summarize.ps1") -Dir $outDir
if ($failed.Count -gt 0) {
  throw "Thresholds breached: $($failed -join ', ')"
}
