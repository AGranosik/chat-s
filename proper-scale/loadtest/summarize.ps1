param(
  [Parameter(Mandatory = $true)]
  [string] $Dir,
  [string] $Compare,
  [double] $WsErrTolerancePct = 0.5
)

$ErrorActionPreference = "Stop"

# Throughput sends begin after ramp + settle; received messages during drain
# count toward completeness. Rates use the measured send-window duration.
function Get-Rows([string] $directory) {
  if (-not (Test-Path $directory)) { throw "no such directory: $directory" }
  $files = Get-ChildItem -Path $directory -Filter *.json -File
  if (-not $files) { throw "no k6 summary JSON files in $directory" }

  $rows = foreach ($file in $files) {
    $summary = Get-Content $file.FullName -Raw | ConvertFrom-Json
    $roomsCount = @($summary.setup_data.rooms).Count
    $usersCount = @($summary.setup_data.users).Count
    $sockets = $roomsCount * $usersCount
    $rampS = if ($null -ne $summary.setup_data.ramp_s) { [int]$summary.setup_data.ramp_s } else { $null }
    $settleS = if ($null -ne $summary.setup_data.settle_s) { [int]$summary.setup_data.settle_s } else { $null }
    $loadS = if ($null -ne $summary.setup_data.duration_s) { [int]$summary.setup_data.duration_s } else { $null }
    $drainS = if ($null -ne $summary.setup_data.drain_s) { [int]$summary.setup_data.drain_s } else { $null }
    $sendIntervalS = if ($null -ne $summary.setup_data.send_interval_s) { [double]$summary.setup_data.send_interval_s } else { $null }

    $passes = 0
    $fails = 0
    $checkBags = @($summary.root_group.checks)
    if ($summary.root_group.groups) {
      $checkBags += @($summary.root_group.groups.PSObject.Properties.Value | ForEach-Object { $_.checks })
    }
    foreach ($bag in $checkBags) {
      if (-not $bag) { continue }
      foreach ($checkValue in $bag.PSObject.Properties.Value) {
        if ($checkValue.name -match 'handshake') {
          $passes += [int]$checkValue.passes
          $fails += [int]$checkValue.fails
        }
      }
    }

    $handshakeTotal = $passes + $fails
    $handshakePct = if ($handshakeTotal -gt 0) { [math]::Round(100.0 * $passes / $handshakeTotal, 1) } else { $null }
    $metrics = $summary.metrics
    $sent = if ($metrics.msgs_sent) { [double]$metrics.msgs_sent.count } else { 0 }
    $sentCount = if ($metrics.msgs_sent) { [long]$metrics.msgs_sent.count } else { 0 }
    $sentRate = if ($metrics.msgs_sent) { [double]$metrics.msgs_sent.rate } else { 0 }
    $acceptedCount = if ($metrics.msgs_accepted) { [long]$metrics.msgs_accepted.count } else { 0 }
    $received = if ($metrics.msgs_received) { [double]$metrics.msgs_received.count } else { 0 }
    $receivedCount = if ($metrics.msgs_received) { [long]$metrics.msgs_received.count } else { 0 }
    $receivedRate = if ($null -ne $loadS -and $loadS -gt 0) { $received / $loadS } elseif ($metrics.msgs_received) { [double]$metrics.msgs_received.rate } else { 0 }
    $wsErrors = if ($metrics.ws_errors) { [int]$metrics.ws_errors.count } else { 0 }
    $connectP90 = if ($metrics.ws_connecting) { [math]::Round([double]$metrics.ws_connecting.'p(90)', 0) } else { $null }
    $connectP95 = if ($metrics.ws_connecting) { [math]::Round([double]$metrics.ws_connecting.'p(95)', 0) } else { $null }
    $e2eP50 = if ($metrics.msg_e2e_latency) { [math]::Round([double]$metrics.msg_e2e_latency.med, 0) } else { $null }
    $e2eP95 = if ($metrics.msg_e2e_latency) { [math]::Round([double]$metrics.msg_e2e_latency.'p(95)', 0) } else { $null }
    $completePct = if ($sent -gt 0 -and $usersCount -gt 0) { [math]::Round(100.0 * $received / ($sent * $usersCount), 1) } else { $null }
    if ($null -ne $loadS -and $loadS -gt 0) { $sentRate = $sentCount / $loadS }
    $expectedDeliveries = [long]($sentCount * $usersCount)
    $wsBudget = [math]::Max(5, [math]::Ceiling($sockets * $WsErrTolerancePct / 100.0))
    $passed = ($wsErrors -le $wsBudget) -and ($null -ne $handshakePct) -and ($handshakePct -ge 99) -and
      (($sent -eq 0) -or ($null -ne $completePct -and $completePct -ge 98))

    [pscustomobject]@{
      file = $file.BaseName
      rooms = $roomsCount
      users_per_room = $usersCount
      total_clients = $sockets
      messages_sent = $sentCount
      messages_accepted = $acceptedCount
      messages_received = $receivedCount
      delivery_success_pct = $completePct
      expected_deliveries = $expectedDeliveries
      mode = $summary.setup_data.mode
      ramp_s = $rampS
      settle_s = $settleS
      load_s = $loadS
      drain_s = $drainS
      send_interval_s = $sendIntervalS
      sockets = $sockets
      room_size = $usersCount
      handshake_pct = $handshakePct
      ws_errors = $wsErrors
      conn_p90_ms = $connectP90
      conn_p95_ms = $connectP95
      e2e_p50_ms = $e2eP50
      e2e_p95_ms = $e2eP95
      sent_count = $sentCount
      accepted_count = $acceptedCount
      sent_per_s = [math]::Round($sentRate, 1)
      recv_per_s = [math]::Round($receivedRate, 1)
      complete_pct = $completePct
      ok = $passed
    }
  }

  if (($rows | Where-Object { $_.sent_per_s -gt 0 }).Count -gt 0) {
    return $rows | Sort-Object sent_per_s
  }
  return $rows | Sort-Object sockets
}

function Show-Table($rows, [string] $title, [string] $outputDirectory) {
  $columns = 'file', 'mode', 'rooms', 'users_per_room', 'total_clients', 'ramp_s', 'settle_s', 'load_s', 'drain_s', 'send_interval_s', 'messages_sent', 'expected_deliveries', 'messages_received', 'delivery_success_pct', 'handshake_pct', 'ws_errors', 'conn_p90_ms', 'conn_p95_ms'
  if (@($rows | Where-Object { $null -ne $_.e2e_p50_ms -or $null -ne $_.e2e_p95_ms }).Count -gt 0) {
    $columns += 'e2e_p50_ms', 'e2e_p95_ms'
  }
  if (@($rows | Where-Object { $_.sent_count -gt 0 -or $_.accepted_count -gt 0 }).Count -gt 0) {
    $columns += 'messages_accepted'
  }
  $columns += 'sent_per_s', 'recv_per_s'
  $lines = @(
    "| $($columns -join ' | ') |",
    "| $(($columns | ForEach-Object { '---' }) -join ' | ') |"
  )
  foreach ($row in $rows) {
    $cells = foreach ($column in $columns) {
      if ($null -eq $row.$column) { '-' } else { "$($row.$column)" }
    }
    $lines += "| $($cells -join ' | ') |"
  }
  $markdown = $lines -join "`n"
  Write-Host "### $title" -ForegroundColor Cyan
  Write-Host $markdown

  $throughputRows = @($rows | Where-Object { $_.sent_per_s -gt 0 })
  if ($throughputRows.Count -gt 0) {
    $best = $throughputRows | Where-Object { $_.ok } | Sort-Object recv_per_s | Select-Object -Last 1
    if ($best) {
      Write-Host "hint: message-rate ceiling ~= $($best.recv_per_s) delivered msg/s at $($best.sockets) sockets ($($best.complete_pct)% complete)." -ForegroundColor Green
    } else {
      Write-Host "hint: no throughput step met the completeness and WebSocket error limits." -ForegroundColor Yellow
    }
  } else {
    $best = $rows | Where-Object { $_.ok } | Sort-Object sockets | Select-Object -Last 1
    if ($best) {
      Write-Host "hint: connection ceiling >= $($best.sockets) sockets." -ForegroundColor Green
    } else {
      Write-Host "hint: no connection step met the handshake and WebSocket error limits." -ForegroundColor Yellow
    }
  }

  if ($outputDirectory) {
    Set-Content -Path (Join-Path $outputDirectory "summary.md") -Value $markdown -Encoding utf8
    $rows | Select-Object $columns | Export-Csv -Path (Join-Path $outputDirectory "summary.csv") -NoTypeInformation
  }
}

$rows = Get-Rows $Dir
Show-Table $rows (Split-Path $Dir -Leaf) $Dir

if ($Compare) {
  $compareRows = Get-Rows $Compare
  Show-Table $compareRows ("compare: " + (Split-Path $Compare -Leaf)) $Compare
  Write-Host "### side-by-side (delivered msg/s by step)" -ForegroundColor Cyan
  Write-Host "| step | sockets | baseline recv/s | compare recv/s |"
  Write-Host "| --- | --- | --- | --- |"
  $keys = @($rows + $compareRows | ForEach-Object { $_.file } | Sort-Object -Unique)
  foreach ($key in $keys) {
    $baseline = $rows | Where-Object file -eq $key | Select-Object -First 1
    $compared = $compareRows | Where-Object file -eq $key | Select-Object -First 1
    $sockets = if ($baseline) { $baseline.sockets } elseif ($compared) { $compared.sockets } else { '-' }
    $baselineRate = if ($baseline) { $baseline.recv_per_s } else { '-' }
    $compareRate = if ($compared) { $compared.recv_per_s } else { '-' }
    Write-Host "| $key | $sockets | $baselineRate | $compareRate |"
  }
}
