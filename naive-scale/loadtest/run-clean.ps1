param(
  [int[]]  $Rooms        = @(10),
  [int[]]  $Users        = @(5),
  [switch] $Full,
  [int]    $Ramp         = 150,
  [int]    $Duration     = 30,
  [int]    $SendInterval = 20,
  [string] $HttpBase     = "http://localhost:80",
  [int]    $ReadyTimeout = 120,
  [switch] $KeepUp
)

$ErrorActionPreference = "Stop"

if ($Full) {
  $Rooms = @(300, 1000)
  $Users = @(10)
}

$repoRoot = Split-Path $PSScriptRoot -Parent
$script   = Join-Path $PSScriptRoot "chat_load.js"
$outDir   = Join-Path $PSScriptRoot "results"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  Write-Error "docker not found on PATH."; exit 1
}
if (-not (Get-Command k6 -ErrorAction SilentlyContinue)) {
  Write-Error "k6 not found on PATH. Install from https://k6.io/docs/get-started/installation/"; exit 1
}

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
  Push-Location $repoRoot; docker compose logs --tail 50 server1 server2 server3 server4 server5 server6 server7; Pop-Location
  exit 1
}
Write-Host "server is up." -ForegroundColor Green

$failed = @()
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
      Write-Warning "$name breached a threshold (k6 exit $LASTEXITCODE) - summary in $summary"
      $failed += $name
    }
  }
}

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
