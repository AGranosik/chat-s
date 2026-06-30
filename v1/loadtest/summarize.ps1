# Reads the k6 --summary-export JSONs from a sweep dir and prints a per-cell
# table you can scan to find where a sweep breaks. Also writes summary.md and
# summary.csv next to the JSONs so you can paste straight into BASELINE.md.
#
#   ./summarize.ps1 -Dir loadtest/results/600m/conn
#   ./summarize.ps1 -Dir loadtest/results/600m/tput
#   ./summarize.ps1 -Dir loadtest/results/1-instance/tput -Compare loadtest/results/3-instance/tput
#
# Per cell it reports: held sockets, room size, handshake success %, ws errors,
# ws_connecting p95, e2e latency p50/p95, offered msg/s (sent) and delivered
# msg/s (received), and delivery completeness % = received / (sent x room_size).
#
# Read-off rules:
#   conn sweep -- the largest socket count with handshake% >= 99 and ws_errors
#                within the teardown budget (see -WsErrTolerancePct) is the
#                connection ceiling.
#   tput sweep -- the highest delivered msg/s with completeness >= 98% and
#                ws_errors within the teardown budget is the message-rate ceiling
#                at that socket count.

param(
  [Parameter(Mandatory = $true)]
  [string] $Dir,
  [string] $Compare,                 # optional: a second sweep dir for a side-by-side
  [double] $WsErrTolerancePct = 0.5  # ws_errors up to this % of sockets are treated as
                                     # k6 end-of-test teardown noise, not a server fault.
                                     # Observed artifact tops out ~0.16% (19/12000); the
                                     # first real collapse is ~22% (2261/10000), so 0.5%
                                     # separates them with wide margin. Set 0 for strict.
)

$ErrorActionPreference = "Stop"

function Get-Rows([string] $d) {
  if (-not (Test-Path $d)) { Write-Error "no such dir: $d"; exit 1 }
  $files = Get-ChildItem -Path $d -Filter *.json -File | Where-Object { $_.Name -ne 'summary.csv' }
  if (-not $files) { Write-Error "no *.json summaries in $d"; exit 1 }

  $rows = foreach ($f in $files) {
    $j = Get-Content $f.FullName -Raw | ConvertFrom-Json

    # Sockets and room size come from setup_data (ROOMS uuids x USERS uuids).
    $roomsN = @($j.setup_data.rooms).Count
    $usersN = @($j.setup_data.users).Count
    $sockets = $roomsN * $usersN

    # Handshake check: scan root + nested groups for the '... handshake ...' check.
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
    $sentRate = if ($m.msgs_sent)     { [double]$m.msgs_sent.rate }      else { 0 }
    $recv     = if ($m.msgs_received) { [double]$m.msgs_received.count } else { 0 }
    $recvRate = if ($m.msgs_received) { [double]$m.msgs_received.rate }  else { 0 }
    $wsErr    = if ($m.ws_errors)     { [int]$m.ws_errors.count }        else { 0 }
    $connP95  = if ($m.ws_connecting) { [math]::Round([double]$m.ws_connecting.'p(95)', 0) } else { $null }
    $e2eP50   = if ($m.msg_e2e_latency) { [math]::Round([double]$m.msg_e2e_latency.med, 0) }    else { $null }
    $e2eP95   = if ($m.msg_e2e_latency) { [math]::Round([double]$m.msg_e2e_latency.'p(95)', 0) } else { $null }

    $complete = if ($sent -gt 0 -and $usersN -gt 0) {
      [math]::Round(100.0 * $recv / ($sent * $usersN), 1)
    } else { $null }

    # k6 tears a few sockets down uncleanly at end-of-test, surfacing as a handful
    # of ws_errors even on a healthy step. Tolerate a budget proportional to socket
    # count (a real collapse is orders of magnitude above it). Floor at 5 so tiny
    # sweeps still absorb the noise.
    $wsBudget = [math]::Max(5, [math]::Ceiling($sockets * $WsErrTolerancePct / 100.0))

    # OK = stayed green: all handshakes, ws errors within budget, and (when sending) ~full delivery.
    $ok = ($wsErr -le $wsBudget) -and ($null -ne $hsPct -and $hsPct -ge 99) -and
          (($sent -eq 0) -or ($null -ne $complete -and $complete -ge 98))

    [pscustomobject]@{
      file          = $f.BaseName
      sockets       = $sockets
      room_size     = $usersN
      handshake_pct = $hsPct
      ws_errors     = $wsErr
      conn_p95_ms   = $connP95
      e2e_p50_ms    = $e2eP50
      e2e_p95_ms    = $e2eP95
      sent_per_s    = [math]::Round($sentRate, 1)
      recv_per_s    = [math]::Round($recvRate, 1)
      complete_pct  = $complete
      ok            = $ok
    }
  }

  # Sort: connection sweeps by sockets, throughput sweeps by offered rate.
  $isTput = ($rows | Where-Object { $_.sent_per_s -gt 0 }).Count -gt 0
  if ($isTput) { $rows | Sort-Object sent_per_s } else { $rows | Sort-Object sockets }
}

function Show-Table($rows, [string] $title, [string] $outDir) {
  $cols = 'file','sockets','room_size','handshake_pct','ws_errors','conn_p95_ms','e2e_p50_ms','e2e_p95_ms','sent_per_s','recv_per_s','complete_pct'

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

  # Read-off hint.
  $isTput = ($rows | Where-Object { $_.sent_per_s -gt 0 }).Count -gt 0
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
