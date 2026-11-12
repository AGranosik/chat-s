param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('conn', 'tput')]
  [string]   $Scenario,
  [Parameter(Mandatory = $true)]
  [double[]] $Steps,
  [int]      $Rooms        = 100,
  [int]      $Users        = 50,
  [int]      $ConnUsers    = 2,
  [ValidatePattern('^[a-zA-Z0-9._-]+$')]
  [string]   $Tag          = "3x33m-proper",
  [int]      $Ramp         = 180,
  [int]      $Settle       = 5,
  [int]      $Duration     = 30,
  [int]      $Drain        = 30,
  [string]   $HttpBase     = "http://localhost:80",
  [int]      $ReadyTimeout = 180,
  [switch]   $NativeK6,
  [switch]   $NoClean,
  [switch]   $KeepUp
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path $PSScriptRoot -Parent
$script = Join-Path $PSScriptRoot "chat_load.js"
$outDir = Join-Path $PSScriptRoot (Join-Path "results" (Join-Path $Tag $Scenario))
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  throw "docker not found on PATH"
}
if ($NativeK6 -and -not (Get-Command k6 -ErrorAction SilentlyContinue)) {
  throw "k6 not found on PATH; it is required with -NativeK6"
}
if ($ConnUsers -lt 1) { throw "ConnUsers must be at least 1" }
if ($Scenario -eq 'tput' -and ($Rooms -lt 1 -or $Users -lt 1)) { throw "Rooms and Users must be at least 1 for tput" }
$invalidSteps = @($Steps | Where-Object { $_ -le 0 })
if ($invalidSteps.Count -gt 0) { throw "Steps must all be greater than zero" }
if ($NativeK6) {
  $maxSockets = if ($Scenario -eq 'conn') { ($Steps | Measure-Object -Maximum).Maximum } else { $Rooms * $Users }
  if ($maxSockets -gt 16000) {
    Write-Warning "Native k6 on Windows may hit the ephemeral-port limit near 16,000 sockets; use containerized k6 or widen the TCP dynamic port range."
  }
}

$serverContainers = @(1..5 | ForEach-Object { "chat-server-$_" })
$readyPath = '/health/ready'

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
  Write-Host "=== wiping the stack (postgres, redis, kafka volumes) and starting it fresh ===" -ForegroundColor Cyan
  Push-Location $repoRoot
  try {
    docker compose down -v --remove-orphans
    docker compose up -d
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed with exit code $LASTEXITCODE" }
  } finally {
    Pop-Location
  }

  if (-not (Wait-Ready $ReadyTimeout)) {
    Push-Location $repoRoot
    try { docker compose logs --tail 80 server1 server2 server3 server4 server5 presence_service delivery_service delivery_service_1 outbox_relay outbox_relay_1 outbox_relay_2 } finally { Pop-Location }
    throw "proper-scale did not become ready at $HttpBase/health/ready within ${ReadyTimeout}s"
  }
}

$started = $false
$failed = @()
try {
  Push-Location $repoRoot
  try {
    docker compose build
    if ($LASTEXITCODE -ne 0) { throw "docker compose build failed with exit code $LASTEXITCODE" }
  } finally {
    Pop-Location
  }

  $started = $true
  Reset-Stack

  $network = $null
  if (-not $NativeK6) {
    Push-Location $repoRoot
    try {
      $networkJson = docker inspect chat-server-1 --format '{{json .NetworkSettings.Networks}}'
      if ($LASTEXITCODE -ne 0) { throw "could not inspect chat-server-1" }
      $network = ($networkJson | ConvertFrom-Json).PSObject.Properties.Name | Select-Object -First 1
    } finally {
      Pop-Location
    }
    if (-not $network) { throw "could not resolve the proper-scale compose network" }
    Write-Host "Containerized k6 will access nginx on network '$network'." -ForegroundColor DarkGray
  }

  $firstStep = $true
  foreach ($step in $Steps) {
    if ($Scenario -eq 'conn') {
      $sockets = [int]$step
      $usersPerRoom = $ConnUsers
      $rooms = [int][Math]::Ceiling($sockets / [double]$usersPerRoom)
      $sendEvery = 0
      $mode = 'conn'
      $name = "conn_{0}" -f $sockets
      $label = "{0} sockets ({1} rooms x {2})" -f ($rooms * $usersPerRoom), $rooms, $usersPerRoom
    } else {
      $rooms = $Rooms
      $usersPerRoom = $Users
      $sendEvery = $step
      $mode = 'tput'
      $name = "tput_si{0}" -f ($step.ToString([System.Globalization.CultureInfo]::InvariantCulture).Replace('.', 'p'))
      $label = "{0} sockets, one message per {1}s/user" -f ($rooms * $usersPerRoom), $step
    }

    if (-not $firstStep -and -not $NoClean) { Reset-Stack }
    $firstStep = $false

    Write-Host "=== [$Scenario] $label (ramp=${Ramp}s, hold=${Duration}s) ===" -ForegroundColor Cyan
    $summaryHost = Join-Path $outDir "$name.json"
    $sendString = ([double]$sendEvery).ToString([System.Globalization.CultureInfo]::InvariantCulture)

    if ($NativeK6) {
      $env:ROOMS = "$rooms"
      $env:USERS = "$usersPerRoom"
      $env:RAMP = "$Ramp"
      $env:SETTLE = "$Settle"
      $env:DURATION = "$Duration"
      $env:DRAIN = "$Drain"
      $env:SEND_INTERVAL = $sendString
      $env:MODE = $mode
      $env:HTTP_BASE = $HttpBase
      k6 run --summary-export $summaryHost $script
      $testExitCode = $LASTEXITCODE
    } else {
      $summaryInner = "/scripts/results/$Tag/$Scenario/$name.json"
      docker run --rm `
        --network $network `
        --sysctl "net.ipv4.ip_local_port_range=1024 65535" `
        --ulimit nofile=262144 `
        -e "ROOMS=$rooms" -e "USERS=$usersPerRoom" -e "RAMP=$Ramp" -e "SETTLE=$Settle" -e "DURATION=$Duration" -e "DRAIN=$Drain" `
        -e "SEND_INTERVAL=$sendString" -e "MODE=$mode" -e "HTTP_BASE=http://chat-nginx:80" `
        -v "${PSScriptRoot}:/scripts" `
        grafana/k6 run --summary-export $summaryInner /scripts/chat_load.js
      $testExitCode = $LASTEXITCODE
    }

    if ($testExitCode -ne 0) {
      Write-Warning "$name breached a threshold; summary: $summaryHost"
      $failed += $name
    }
  }
} finally {
  if ($started -and -not $KeepUp) {
    Push-Location $repoRoot
    try { docker compose down -v --remove-orphans } finally { Pop-Location }
  }
}

Write-Host "Summaries: $outDir" -ForegroundColor Green
Write-Host "=== Load-test summary ===" -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "summarize.ps1") -Dir $outDir
if ($failed.Count -gt 0) {
  throw "Thresholds breached: $($failed -join ', ')"
}
