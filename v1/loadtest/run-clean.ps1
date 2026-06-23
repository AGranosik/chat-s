# Runs the chat-s load test against a guaranteed-clean database.
#
# It recreates the Docker stack (postgres has no volume, so a fresh container =
# empty DB; the server re-applies migrations on startup), waits for the server
# to become healthy, then runs the ROOMS x USERS load matrix. Per-run JSON
# summaries land in loadtest/results/.
#
#   ./run-clean.ps1                              # small smoke run (10 rooms x 5 users)
#   ./run-clean.ps1 -Full                        # full 4x3 matrix (up to 10k sockets!)
#   ./run-clean.ps1 -Rooms 1,10 -Users 2,5       # custom matrix
#   ./run-clean.ps1 -KeepUp                       # leave the stack running afterwards
#
# Run with PowerShell 7 if possible:  pwsh -File ./run-clean.ps1
#
# The largest cell (1000 rooms x 10 users) opens 10,000 sockets from this host;
# raise the OS file-descriptor limit before running -Full.

param(
  [int[]]  $Rooms        = @(10),
  [int[]]  $Users        = @(5),
  [switch] $Full,                                  # override Rooms/Users with the full matrix
  [int]    $Duration     = 60,
  [int]    $SendInterval = 20,
  [string] $HttpBase     = "http://localhost:80",
  [int]    $ReadyTimeout = 120,                    # seconds to wait for /healthz
  [switch] $KeepUp                                 # don't tear the stack down at the end
)

$ErrorActionPreference = "Stop"

if ($Full) {
  $Rooms = @(10, 100, 100, 300)
  $Users = @(5, 10)
}

$repoRoot = Split-Path $PSScriptRoot -Parent
$script   = Join-Path $PSScriptRoot "chat_load.js"
$outDir   = Join-Path $PSScriptRoot "results"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

# ---- Preflight -------------------------------------------------------------
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  Write-Error "docker not found on PATH."; exit 1
}
if (-not (Get-Command k6 -ErrorAction SilentlyContinue)) {
  Write-Error "k6 not found on PATH. Install from https://k6.io/docs/get-started/installation/"; exit 1
}

# ---- Clean stack -----------------------------------------------------------
Write-Host "=== recreating a clean stack (wipes existing DB) ===" -ForegroundColor Cyan
Push-Location $repoRoot
try {
  docker compose down --remove-orphans
  docker compose up -d --build
  if ($LASTEXITCODE -ne 0) { Write-Error "docker compose up failed (exit $LASTEXITCODE)"; exit 1 }
}
finally {
  Pop-Location
}

# ---- Wait for readiness ----------------------------------------------------
Write-Host "=== waiting for $HttpBase/healthz (up to ${ReadyTimeout}s) ===" -ForegroundColor Cyan
$deadline = (Get-Date).AddSeconds($ReadyTimeout)
$ready    = $false
while ((Get-Date) -lt $deadline) {
  try {
    $resp = Invoke-WebRequest "$HttpBase/healthz" -UseBasicParsing -TimeoutSec 5
    if ($resp.StatusCode -eq 200) { $ready = $true; break }
  } catch {
    Start-Sleep -Seconds 2
  }
}
if (-not $ready) {
  Write-Error "server never became healthy within ${ReadyTimeout}s"
  Push-Location $repoRoot; docker compose logs --tail 50 server; Pop-Location
  exit 1
}
Write-Host "server is up." -ForegroundColor Green

# ---- Run the matrix --------------------------------------------------------
$failed = @()
foreach ($r in $Rooms) {
  foreach ($u in $Users) {
    $name = "rooms{0}_users{1}" -f $r, $u
    $vus  = $r * $u
    Write-Host ""
    Write-Host "=== $name  (VUs=$vus, duration=${Duration}s) ===" -ForegroundColor Cyan

    $env:ROOMS         = $r
    $env:USERS         = $u
    $env:DURATION      = $Duration
    $env:SEND_INTERVAL = $SendInterval
    $env:HTTP_BASE     = $HttpBase

    $summary = Join-Path $outDir "$name.json"
    k6 run --summary-export $summary $script

    if ($LASTEXITCODE -ne 0) {
      Write-Warning "$name breached a threshold (k6 exit $LASTEXITCODE) - summary in $summary"
      $failed += $name
    }
  }
}

# ---- Teardown --------------------------------------------------------------
if (-not $KeepUp) {
  Write-Host ""
  Write-Host "=== tearing down stack (pass -KeepUp to keep it) ===" -ForegroundColor Cyan
  Push-Location $repoRoot; docker compose down --remove-orphans; Pop-Location
}

Write-Host ""
if ($failed.Count -gt 0) {
  Write-Host "Done with $($failed.Count) threshold breach(es): $($failed -join ', ')" -ForegroundColor Yellow
} else {
  Write-Host "Done. All scenarios within thresholds." -ForegroundColor Green
}
Write-Host "Per-run summaries in $outDir" -ForegroundColor Green
