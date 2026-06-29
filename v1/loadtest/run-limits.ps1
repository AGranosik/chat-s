# Limit-finding sweep runner for the chat-s server.
#
# Two coupled questions, one fixed 600m memory budget (set in docker-compose.yml):
#
#   1. How many websockets fit?   -Scenario conn  -Steps 5000,10000,20000,...
#        Each step holds that many sockets and sends nothing. The largest step
#        that stays GREEN (k6 exit 0: all handshakes ok, no ws errors, server not
#        OOM-restarted) is the connection ceiling.
#
#   2. What message rate fits at that ceiling?   -Scenario tput
#        Hold the socket count you found (-Rooms x -Users) and sweep the per-VU
#        send interval (-Steps = SEND_INTERVAL values, fractional ok). Read the
#        max delivered rate at >=98% completeness from summarize.ps1.
#
# The generator is a CONTAINERIZED k6 on the compose network by default, so the
# Windows ephemeral-port wall (~16k) never caps the test below the server. Pass
# -NativeK6 to use the host k6 binary instead (then widen the port range first --
# see README).
#
#   ./run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000
#   ./run-limits.ps1 -Scenario tput -Rooms 400 -Users 25 -Steps 5,2,1,0.5,0.25
#
# Per-step JSON summaries land in loadtest/results/<Tag>/<Scenario>/. Feed that
# dir to summarize.ps1 to read off where each sweep breaks. This script only runs
# the sweep -- you decide where the limit is.

param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('conn', 'tput')]
  [string]   $Scenario,

  [Parameter(Mandatory = $true)]
  [double[]] $Steps,                              # conn: socket counts; tput: SEND_INTERVAL values

  [int]      $Rooms        = 100,                 # tput only: fixed room count (set to your found limit)
  [int]      $Users        = 50,                  # tput only: users per room (Rooms*Users = held sockets)
  [int]      $ConnUsers    = 2,                   # conn only: users/room (small = minimal fan-out)

  [string]   $Tag          = "600m",
  [int]      $Ramp         = 180,                 # ramp sockets up over N seconds (avoid a thundering herd)
  [int]      $Duration     = 30,                  # hold-at-full-load window, seconds

  [string]   $HttpBase     = "http://localhost:80",
  [int]      $ReadyTimeout = 120,
  [switch]   $NativeK6,                           # use the host k6 binary instead of a container
  [switch]   $NoRestart,                          # don't restart the server between steps (default: fresh process per step)
  [switch]   $KeepUp                              # leave the stack up at the end
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path $PSScriptRoot -Parent
$script   = Join-Path $PSScriptRoot "chat_load.js"
$outDir   = Join-Path $PSScriptRoot (Join-Path "results" (Join-Path $Tag $Scenario))
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

# ---- Preflight -------------------------------------------------------------
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  Write-Error "docker not found on PATH."; exit 1
}
if ($NativeK6) {
  if (-not (Get-Command k6 -ErrorAction SilentlyContinue)) {
    Write-Error "k6 not found on PATH (required with -NativeK6). Install from https://k6.io/docs/get-started/installation/"; exit 1
  }
  $maxSockets = if ($Scenario -eq 'conn') { ($Steps | Measure-Object -Maximum).Maximum } else { $Rooms * $Users }
  if ($maxSockets -gt 16000) {
    Write-Warning ("native k6 on Windows draws ~16k ephemeral ports; this run wants {0} sockets. " +
      "Widen the range first (admin): netsh int ipv4 set dynamicport tcp start=10000 num=55000 -- " +
      "or drop -NativeK6 to use the containerized generator." -f $maxSockets)
  }
}

function Wait-Healthy([int] $TimeoutSec) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      $resp = Invoke-WebRequest "$HttpBase/healthz" -UseBasicParsing -TimeoutSec 5
      if ($resp.StatusCode -eq 200) { return $true }
    } catch { Start-Sleep -Seconds 2 }
  }
  return $false
}

# ---- Clean stack -----------------------------------------------------------
Write-Host "=== recreating a clean stack (600m server budget; wipes existing DB) ===" -ForegroundColor Cyan
Push-Location $repoRoot
try {
  docker compose down --remove-orphans
  docker compose up -d --build
  if ($LASTEXITCODE -ne 0) { Write-Error "docker compose up failed (exit $LASTEXITCODE)"; exit 1 }
}
finally { Pop-Location }

Write-Host "=== waiting for $HttpBase/healthz (up to ${ReadyTimeout}s) ===" -ForegroundColor Cyan
if (-not (Wait-Healthy $ReadyTimeout)) {
  Write-Error "server never became healthy within ${ReadyTimeout}s"
  Push-Location $repoRoot; docker compose logs --tail 50 server; Pop-Location
  exit 1
}
Write-Host "server is up." -ForegroundColor Green

# ---- Resolve the containerized-generator network ---------------------------
$net = $null
if (-not $NativeK6) {
  $netsJson = docker inspect chat-server --format '{{json .NetworkSettings.Networks}}'
  $net = ($netsJson | ConvertFrom-Json).PSObject.Properties.Name | Select-Object -First 1
  if (-not $net) { Write-Error "could not resolve the compose network for chat-server"; exit 1 }
  Write-Host "containerized k6 -> network '$net' -> http://chat-nginx:80" -ForegroundColor DarkGray
}

# ---- Run the sweep ---------------------------------------------------------
$failed = @()
foreach ($s in $Steps) {
  # Map a step onto the k6 knobs for this scenario.
  if ($Scenario -eq 'conn') {
    $sockets   = [int]$s
    $u         = $ConnUsers
    $r         = [int][Math]::Ceiling($sockets / [double]$u)
    $sendEvery = 0
    $mode      = 'conn'
    $name      = "conn_{0}" -f $sockets
    $label     = "{0} sockets ({1} rooms x {2})" -f ($r * $u), $r, $u
  }
  else {
    $r         = $Rooms
    $u         = $Users
    $sendEvery = $s
    $mode      = 'tput'
    $name      = "tput_si{0}" -f ($s.ToString([System.Globalization.CultureInfo]::InvariantCulture).Replace('.', 'p'))
    $label     = "{0} sockets, 1 msg / {1}s/VU (offered ~{2:n0} msg/s)" -f ($r * $u), $s, (($r * $u) / $s)
  }

  # Fresh Go process per step so heap/goroutines from a prior step don't skew it.
  if (-not $NoRestart) {
    Push-Location $repoRoot; docker compose restart server | Out-Null; Pop-Location
    if (-not (Wait-Healthy $ReadyTimeout)) { Write-Error "server unhealthy after restart"; exit 1 }
  }

  Write-Host ""
  Write-Host "=== [$Scenario] $label  (ramp=${Ramp}s, hold=${Duration}s) ===" -ForegroundColor Cyan

  $summaryHost = Join-Path $outDir "$name.json"
  $sendStr     = ([double]$sendEvery).ToString([System.Globalization.CultureInfo]::InvariantCulture)

  if ($NativeK6) {
    $env:ROOMS         = $r
    $env:USERS         = $u
    $env:RAMP          = $Ramp
    $env:DURATION      = $Duration
    $env:SEND_INTERVAL = $sendStr
    $env:MODE          = $mode
    $env:HTTP_BASE     = $HttpBase
    k6 run --summary-export $summaryHost $script
  }
  else {
    # /scripts is the mounted loadtest dir, so the summary written under
    # /scripts/results/... lands back on the host in $outDir.
    $summaryInner = "/scripts/results/$Tag/$Scenario/$name.json"
    docker run --rm `
      --network $net `
      --sysctl "net.ipv4.ip_local_port_range=1024 65535" `
      --ulimit nofile=262144 `
      -e ROOMS=$r -e USERS=$u -e RAMP=$Ramp -e DURATION=$Duration `
      -e SEND_INTERVAL=$sendStr -e MODE=$mode -e HTTP_BASE=http://chat-nginx:80 `
      -v "${PSScriptRoot}:/scripts" `
      grafana/k6 run --summary-export $summaryInner /scripts/chat_load.js
  }

  if ($LASTEXITCODE -ne 0) {
    Write-Warning "$name breached a threshold (k6 exit $LASTEXITCODE) - summary in $summaryHost"
    $failed += $name
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
  Write-Host "Steps that breached a threshold: $($failed -join ', ')" -ForegroundColor Yellow
} else {
  Write-Host "All steps stayed within thresholds." -ForegroundColor Green
}
Write-Host "Summaries in $outDir" -ForegroundColor Green
Write-Host "Read them with:  ./loadtest/summarize.ps1 -Dir `"$outDir`"" -ForegroundColor Green
