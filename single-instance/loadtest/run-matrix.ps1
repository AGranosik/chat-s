# Runs the full chat-s load-test matrix: ROOMS x USERS. Connections ramp up over
# -Ramp seconds, hold at full load for -Duration seconds, and each user sends one
# message every -SendInterval seconds.
#
# Per-run JSON summaries land in loadtest/results/. A non-zero k6 exit code means
# that run breached a threshold; the script reports it and keeps going.
#
#   ./run-matrix.ps1                                   # full 4x3 matrix
#   ./run-matrix.ps1 -Rooms 1,10 -Users 2,5 -Duration 60
#
# The largest cell (1000 rooms x 10 users) opens 10,000 sockets from this host;
# raise the OS file-descriptor limit and run against a warm Postgres.

param(
  [int[]]  $Rooms        = @(10, 100, 100),
  [int[]]  $Users        = @(5, 10, 100),
  [int]    $Ramp         = 150,
  [int]    $Duration     = 30,
  [int]    $SendInterval = 20,
  [string] $HttpBase     = "http://localhost:80"
)

$script  = Join-Path $PSScriptRoot "chat_load.js"
$outDir  = Join-Path $PSScriptRoot "results"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

if (-not (Get-Command k6 -ErrorAction SilentlyContinue)) {
  Write-Error "k6 not found on PATH. Install from https://k6.io/docs/get-started/installation/"
  exit 1
}

foreach ($r in $Rooms) {
  foreach ($u in $Users) {
    $name = "rooms{0}_users{1}" -f $r, $u
    $vus  = $r * $u
    Write-Host ""
    Write-Host "=== $name  (VUs=$vus, ramp=${Ramp}s, hold=${Duration}s) ===" -ForegroundColor Cyan

    $env:ROOMS         = $r
    $env:USERS         = $u
    $env:RAMP          = $Ramp
    $env:DURATION      = $Duration
    $env:SEND_INTERVAL = $SendInterval
    $env:HTTP_BASE     = $HttpBase

    $summary = Join-Path $outDir "$name.json"
    k6 run --summary-export $summary $script

    if ($LASTEXITCODE -ne 0) {
      Write-Warning "$name breached a threshold (k6 exit $LASTEXITCODE) — summary in $summary"
    }
  }
}

Write-Host ""
Write-Host "Done. Per-run summaries in $outDir" -ForegroundColor Green
