param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('conn', 'tput')]
  [string]   $Scenario,

  [Parameter(Mandatory = $true)]
  [double[]] $Steps,

  [int]      $Rooms        = 100,
  [int]      $Users        = 50,
  [int]      $ConnUsers    = 2,

  [string]   $Tag          = "300m",
  [int]      $Ramp         = 180,
  [int]      $Settle       = 5,
  [int]      $Duration     = 30,
  [int]      $Drain        = 30,

  [string]   $HttpBase     = "http://localhost:80",
  [int]      $ReadyTimeout = 120,
  [switch]   $NativeK6,
  [switch]   $NoClean,
  [switch]   $KeepUp
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path $PSScriptRoot -Parent
$script   = Join-Path $PSScriptRoot "chat_load.js"
$outDir   = Join-Path $PSScriptRoot (Join-Path "results" (Join-Path $Tag $Scenario))
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

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

$serverContainers = @('chat-server')
$readyPath = '/healthz'

function Wait-Ready([int] $TimeoutSec) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    $pending = @()
    foreach ($container in $serverContainers) {
      docker exec $container sh -c "wget -q -O /dev/null -T 2 http://127.0.0.1:8080$readyPath >/dev/null 2>&1" | Out-Null
      if ($LASTEXITCODE -ne 0) { $pending += $container }
    }
    if ($pending.Count -eq 0) {
      try {
        $response = Invoke-WebRequest "$($HttpBase.TrimEnd('/'))$readyPath" -UseBasicParsing -TimeoutSec 5
        if ($response.StatusCode -eq 200) { return $true }
      } catch { }
    }
    Start-Sleep -Seconds 1
  }
  return $false
}

function Reset-Stack {
  Write-Host "=== wiping the stack (postgres volume) and starting it fresh ===" -ForegroundColor Cyan
  Push-Location $repoRoot
  try {
    docker compose down -v --remove-orphans
    docker compose up -d
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed (exit $LASTEXITCODE)" }
  }
  finally { Pop-Location }

  Write-Host "=== waiting for chat-server and $HttpBase$readyPath (up to ${ReadyTimeout}s) ===" -ForegroundColor Cyan
  if (-not (Wait-Ready $ReadyTimeout)) {
    Push-Location $repoRoot; docker compose logs --tail 50 server; Pop-Location
    throw "server never became healthy within ${ReadyTimeout}s"
  }
  Write-Host "server is up." -ForegroundColor Green
}

Push-Location $repoRoot
try {
  docker compose build
  if ($LASTEXITCODE -ne 0) { throw "docker compose build failed (exit $LASTEXITCODE)" }
}
finally { Pop-Location }

Reset-Stack

$net = $null
if (-not $NativeK6) {
  $netsJson = docker inspect chat-server --format '{{json .NetworkSettings.Networks}}'
  $net = ($netsJson | ConvertFrom-Json).PSObject.Properties.Name | Select-Object -First 1
  if (-not $net) { Write-Error "could not resolve the compose network for chat-server"; exit 1 }
  Write-Host "containerized k6 -> network '$net' -> http://chat-nginx:80" -ForegroundColor DarkGray
}

$failed = @()
$firstStep = $true
foreach ($s in $Steps) {
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

  if (-not $firstStep -and -not $NoClean) { Reset-Stack }
  $firstStep = $false

  Write-Host ""
  Write-Host "=== [$Scenario] $label  (ramp=${Ramp}s, hold=${Duration}s) ===" -ForegroundColor Cyan

  $summaryHost = Join-Path $outDir "$name.json"
  $sendStr     = ([double]$sendEvery).ToString([System.Globalization.CultureInfo]::InvariantCulture)

  if ($NativeK6) {
    $env:ROOMS         = $r
    $env:USERS         = $u
    $env:RAMP          = $Ramp
    $env:SETTLE        = $Settle
    $env:DURATION      = $Duration
    $env:DRAIN         = $Drain
    $env:SEND_INTERVAL = $sendStr
    $env:MODE          = $mode
    $env:HTTP_BASE     = $HttpBase
    k6 run --summary-export $summaryHost $script
  }
  else {
    $summaryInner = "/scripts/results/$Tag/$Scenario/$name.json"
    docker run --rm `
      --network $net `
      --sysctl "net.ipv4.ip_local_port_range=1024 65535" `
      --ulimit nofile=262144 `
      -e ROOMS=$r -e USERS=$u -e RAMP=$Ramp -e SETTLE=$Settle -e DURATION=$Duration -e DRAIN=$Drain `
      -e SEND_INTERVAL=$sendStr -e MODE=$mode -e HTTP_BASE=http://chat-nginx:80 `
      -v "${PSScriptRoot}:/scripts" `
      grafana/k6 run --summary-export $summaryInner /scripts/chat_load.js
  }

  if ($LASTEXITCODE -ne 0) {
    Write-Warning "$name breached a threshold (k6 exit $LASTEXITCODE) - summary in $summaryHost"
    $failed += $name
  }
}

if (-not $KeepUp) {
  Write-Host ""
  Write-Host "=== tearing down stack and its volumes (pass -KeepUp to keep it) ===" -ForegroundColor Cyan
  Push-Location $repoRoot; docker compose down -v --remove-orphans; Pop-Location
}

Write-Host ""
if ($failed.Count -gt 0) {
  Write-Host "Steps that breached a threshold: $($failed -join ', ')" -ForegroundColor Yellow
} else {
  Write-Host "All steps stayed within thresholds." -ForegroundColor Green
}
Write-Host "Summaries in $outDir" -ForegroundColor Green
Write-Host "Read them with:  ./loadtest/summarize.ps1 -Dir `"$outDir`"" -ForegroundColor Green
