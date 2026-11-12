param(
  [Parameter(Mandatory = $true)]
  [string] $Dir,
  [string] $Compare,
  [double] $WsErrTolerancePct = 0.5,
  [double] $LatencyP95Ms = 2500
)

$ErrorActionPreference = "Stop"

function Get-Rows([string] $d) {
  if (-not (Test-Path $d)) { Write-Error "no such dir: $d"; exit 1 }
  $files = Get-ChildItem -Path $d -Filter *.json -File | Where-Object { $_.Name -ne 'summary.csv' }
  if (-not $files) { Write-Error "no *.json summaries in $d"; exit 1 }

  $rows = foreach ($f in $files) {
    $j = Get-Content $f.FullName -Raw | ConvertFrom-Json

    $roomsN = @($j.setup_data.rooms).Count
    $usersN = @($j.setup_data.users).Count
    $sockets = $roomsN * $usersN
    $rampS = if ($null -ne $j.setup_data.ramp_s) { [int]$j.setup_data.ramp_s } else { $null }
    $settleS = if ($null -ne $j.setup_data.settle_s) { [int]$j.setup_data.settle_s } else { $null }
    $loadS = if ($null -ne $j.setup_data.duration_s) { [int]$j.setup_data.duration_s } else { $null }
    $drainS = if ($null -ne $j.setup_data.drain_s) { [int]$j.setup_data.drain_s } else { $null }
    $sendIntervalS = if ($null -ne $j.setup_data.send_interval_s) { [double]$j.setup_data.send_interval_s } else { $null }

    $passes = 0; $fails = 0
    $checkBags = @($j.root_group.checks)
    foreach ($g in @($j.root_group.groups.PSObject.Properties.Value)) { $checkBags += $g.checks }
    foreach ($bag in $checkBags) {
      if (-not $bag) { continue }
      foreach ($c in $bag.PSObject.Properties.Value) {
        if ($c.name -match 'handshake') { $passes += [int]$c.passes; $fails += [int]$c.fails }
      }
    }
    $hsTotal = $passes + $fails
    $hsPct   = if ($hsTotal -gt 0) { [math]::Round(100.0 * $passes / $hsTotal, 1) } else { $null }

    $m        = $j.metrics
    $sent     = if ($m.msgs_sent)     { [double]$m.msgs_sent.count }     else { 0 }
    $sentCount = if ($m.msgs_sent)    { [long]$m.msgs_sent.count }       else { 0 }
    $sentRate = if ($m.msgs_sent)     { [double]$m.msgs_sent.rate }      else { 0 }
    $recv     = if ($m.msgs_received) { [double]$m.msgs_received.count } else { 0 }
    $recvCount = if ($m.msgs_received) { [long]$m.msgs_received.count }   else { 0 }
    $e2eP50   = if ($m.msg_e2e_latency) { [math]::Round([double]$m.msg_e2e_latency.med, 0) }    else { $null }
    $e2eP95   = if ($m.msg_e2e_latency) { [math]::Round([double]$m.msg_e2e_latency.'p(95)', 0) } else { $null }
    $e2eMax   = if ($m.msg_e2e_latency) { [math]::Round([double]$m.msg_e2e_latency.max, 0) }    else { $null }

    $deliverS = $loadS
    if ($null -ne $loadS -and $null -ne $e2eP95 -and $e2eP95 -ge $LatencyP95Ms) {
      $backlogS = $e2eMax / 1000.0
      if ($null -ne $drainS) { $backlogS = [math]::Min($backlogS, [double]$drainS) }
      $deliverS = $loadS + $backlogS
    }

    $recvRate = if ($null -ne $deliverS -and $deliverS -gt 0) { $recv / $deliverS } elseif ($m.msgs_received) { [double]$m.msgs_received.rate } else { 0 }
    if ($null -ne $loadS -and $loadS -gt 0) { $sentRate = $sentCount / $loadS }
    $expectedDeliveries = [long]($sentCount * $usersN)
    $wsErr    = if ($m.ws_errors)     { [int]$m.ws_errors.count }        else { 0 }
    $connP95  = if ($m.ws_connecting) { [math]::Round([double]$m.ws_connecting.'p(95)', 0) } else { $null }

    $complete = if ($sent -gt 0 -and $usersN -gt 0) {
      [math]::Round(100.0 * $recv / ($sent * $usersN), 1)
    } else { $null }

    $wsBudget = [math]::Max(5, [math]::Ceiling($sockets * $WsErrTolerancePct / 100.0))

    $ok = ($wsErr -le $wsBudget) -and ($null -ne $hsPct -and $hsPct -ge 99) -and
          (($sent -eq 0) -or ($null -ne $complete -and $complete -ge 98))

    [pscustomobject]@{
      file          = $f.BaseName
      rooms         = $roomsN
      users_per_room = $usersN
      total_clients = $sockets
      messages_sent = $sentCount
      messages_received = $recvCount
      delivery_success_pct = $complete
      sockets       = $sockets
      room_size     = $usersN
      handshake_pct = $hsPct
      ws_errors     = $wsErr
      conn_p95_ms   = $connP95
      e2e_p50_ms    = $e2eP50
      e2e_p95_ms    = $e2eP95
      e2e_max_ms    = $e2eMax
      sent_per_s    = [math]::Round($sentRate, 1)
      deliver_s     = if ($null -ne $deliverS) { [math]::Round($deliverS, 1) } else { $null }
      recv_per_s    = [math]::Round($recvRate, 1)
      mode          = $j.setup_data.mode
      ramp_s        = $rampS
      settle_s      = $settleS
      load_s        = $loadS
      drain_s       = $drainS
      send_interval_s = $sendIntervalS
      expected_deliveries = $expectedDeliveries
      complete_pct  = $complete
      ok            = $ok
    }
  }

  $isTput = @($rows | Where-Object { $_.sent_per_s -gt 0 }).Count -gt 0
  if ($isTput) { $rows | Sort-Object sent_per_s } else { $rows | Sort-Object sockets }
}

function Show-Table($rows, [string] $title, [string] $outDir) {
  $cols = 'file','mode','rooms','users_per_room','total_clients','ramp_s','settle_s','load_s','drain_s','send_interval_s','messages_sent','expected_deliveries','messages_received','delivery_success_pct','handshake_pct','ws_errors','conn_p95_ms','e2e_p50_ms','e2e_p95_ms','e2e_max_ms','sent_per_s','deliver_s','recv_per_s'

  $header = "| " + ($cols -join " | ") + " |"
  $sep    = "| " + (($cols | ForEach-Object { "---" }) -join " | ") + " |"
  $lines  = @($header, $sep)
  foreach ($r in $rows) {
    $cells = foreach ($c in $cols) { $v = $r.$c; if ($null -eq $v) { "-" } else { "$v" } }
    $lines += "| " + ($cells -join " | ") + " |"
  }
  $md = $lines -join "`n"

  Write-Host ""
  Write-Host "### $title" -ForegroundColor Cyan
  Write-Host $md

  $isTput = @($rows | Where-Object { $_.sent_per_s -gt 0 }).Count -gt 0
  if ($isTput) {
    $best = $rows | Where-Object { $_.ok } | Sort-Object recv_per_s | Select-Object -Last 1
    if ($best) {
      Write-Host ("hint: message-rate ceiling ~= {0} delivered msg/s at {1} sockets (completeness {2}%)." -f `
        $best.recv_per_s, $best.sockets, $best.complete_pct) -ForegroundColor Green
    } else { Write-Host "hint: no step stayed green (>=98% complete, ws errors within teardown budget)." -ForegroundColor Yellow }
  } else {
    $best = $rows | Where-Object { $_.ok } | Sort-Object sockets | Select-Object -Last 1
    if ($best) {
      Write-Host ("hint: connection ceiling >= {0} sockets (largest all-green step)." -f $best.sockets) -ForegroundColor Green
    } else { Write-Host "hint: no step stayed green (100% handshakes, ws errors within teardown budget)." -ForegroundColor Yellow }
  }

  if ($outDir) {
    Set-Content -Path (Join-Path $outDir "summary.md") -Value $md -Encoding utf8
    $rows | Select-Object $cols | Export-Csv -Path (Join-Path $outDir "summary.csv") -NoTypeInformation
  }
}

$rows = Get-Rows $Dir
Show-Table $rows (Split-Path $Dir -Leaf) $Dir

if ($Compare) {
  $cmp = Get-Rows $Compare
  Show-Table $cmp ("compare: " + (Split-Path $Compare -Leaf)) $Compare

  Write-Host ""
  Write-Host "### side-by-side (delivered msg/s by step)" -ForegroundColor Cyan
  $keys = ($rows + $cmp | ForEach-Object { $_.file } | Sort-Object -Unique)
  Write-Host "| step | sockets | baseline recv/s | compare recv/s |"
  Write-Host "| --- | --- | --- | --- |"
  foreach ($k in $keys) {
    $a = $rows | Where-Object file -eq $k | Select-Object -First 1
    $b = $cmp  | Where-Object file -eq $k | Select-Object -First 1
    $sk = if ($a) { $a.sockets } elseif ($b) { $b.sockets } else { "-" }
    $av = if ($a) { $a.recv_per_s } else { "-" }
    $bv = if ($b) { $b.recv_per_s } else { "-" }
    Write-Host "| $k | $sk | $av | $bv |"
  }
}
