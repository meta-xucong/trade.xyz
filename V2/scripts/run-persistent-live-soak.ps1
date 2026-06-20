param(
    [int]$WindowSecs = 600,
    [int]$MaxRounds = 0,
    [int]$SleepSecs = 5,
    [int]$DegradedSleepSecs = 60,
    [int]$RoundTimeoutBufferSecs = 1800,
    [double]$MaxTotalNotionalUsd = 200.0,
    [double]$MaxTotalFeesUsd = 1.0,
    [int]$MaxEvents = 20000,
    [switch]$HoldPositionsAfterSubmit,
    [switch]$StopAfterRealSubmit,
    [ValidateSet("all", "swing")]
    [string]$LeaderSet = "swing",
    [string]$SettingsPath = ".codex-longrun\copy-ui-settings.json",
    [string]$PersistencePath = "",
    [string]$ShadowPath = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $projectRoot

if (-not $env:TRADE_XYZ_VAULT_PASSWORD) {
    throw "TRADE_XYZ_VAULT_PASSWORD must be set in the launching environment"
}

$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$prefix = ".codex-longrun\persistent-live-soak-$runId"
$summaryPath = "$prefix-summary.jsonl"
$logPath = "$prefix-run.log"
$notificationSettingsPath = ".codex-longrun\notification-settings.json"
if ([string]::IsNullOrWhiteSpace($PersistencePath)) {
    $persistencePath = "$prefix-snapshot.json"
} else {
    $persistencePath = $PersistencePath
}
if ([string]::IsNullOrWhiteSpace($ShadowPath)) {
    $shadowPath = "$prefix-shadow.jsonl"
} else {
    $shadowPath = $ShadowPath
}

function Write-SoakLog {
    param([string]$Message)
    $line = "$(Get-Date -Format o) $Message"
    Add-Content -LiteralPath $logPath -Value $line -Encoding utf8
    Write-Host $line
}

function Get-NotificationSettings {
    if (-not (Test-Path -LiteralPath $notificationSettingsPath)) {
        return $null
    }
    try {
        $settings = Get-Content -LiteralPath $notificationSettingsPath -Raw -Encoding utf8 | ConvertFrom-Json
        if (-not [bool]$settings.enabled) {
            return $null
        }
        return $settings
    } catch {
        Write-SoakLog "notification settings parse failed: $($_.Exception.Message)"
        return $null
    }
}

function Send-StopNotification {
    param(
        [string]$Status,
        [string]$Reason,
        [string]$Detail
    )
    $settings = Get-NotificationSettings
    if ($null -eq $settings) {
        return
    }
    $title = "trade.xyz copy live soak $Status"
    $message = @(
        "run_id=$runId",
        "status=$Status",
        "reason=$Reason",
        "detail=$Detail",
        "time=$(Get-Date -Format o)"
    ) -join "`n"
    try {
        if ([string]$settings.provider -eq "feishu") {
            $webhook = [string]$settings.feishu_webhook
            if ([string]::IsNullOrWhiteSpace($webhook)) {
                return
            }
            $body = @{
                msg_type = "text"
                content = @{ text = "$title`n$message" }
            } | ConvertTo-Json -Compress -Depth 6
            Invoke-RestMethod -Uri $webhook -Method POST -ContentType "application/json" -Body $body -TimeoutSec 10 | Out-Null
            Write-SoakLog "stop notification sent provider=feishu status=$Status reason=$Reason"
        } else {
            $sendKey = [string]$settings.serverchan_sendkey
            if ([string]::IsNullOrWhiteSpace($sendKey)) {
                return
            }
            $uri = "https://sctapi.ftqq.com/$sendKey.send"
            Invoke-RestMethod -Uri $uri -Method POST -ContentType "application/x-www-form-urlencoded" -Body @{
                title = $title
                desp = $message
                short = $Reason
                noip = "1"
            } -TimeoutSec 10 | Out-Null
            Write-SoakLog "stop notification sent provider=serverchan status=$Status reason=$Reason"
        }
    } catch {
        $errorMessage = [string]$_.Exception.Message
        if ($null -ne $settings.serverchan_sendkey) {
            $errorMessage = $errorMessage.Replace([string]$settings.serverchan_sendkey, "***")
        }
        if ($null -ne $settings.feishu_webhook) {
            $errorMessage = $errorMessage.Replace([string]$settings.feishu_webhook, "***")
        }
        Write-SoakLog "stop notification failed provider=$($settings.provider) status=$Status error=$errorMessage"
    }
}

function Stop-WithNotification {
    param(
        [int]$Code,
        [string]$Status,
        [string]$Reason,
        [string]$Detail = ""
    )
    Send-StopNotification -Status $Status -Reason $Reason -Detail $Detail
    exit $Code
}

Write-SoakLog "starting persistent live soak window_secs=$WindowSecs max_rounds=$MaxRounds max_notional=$MaxTotalNotionalUsd max_fees=$MaxTotalFeesUsd hold_positions_after_submit=$([bool]$HoldPositionsAfterSubmit) settings=$SettingsPath persistence=$persistencePath shadow=$shadowPath"
$holdPositionsArg = ([bool]$HoldPositionsAfterSubmit).ToString().ToLowerInvariant()
$botExe = Join-Path $env:USERPROFILE ".cargo\target-trade_xyz_bot\debug\trade_xyz_bot_v2.exe"
if (-not (Test-Path -LiteralPath $botExe)) {
    throw "trade_xyz_bot_v2.exe not found at $botExe; run cargo build --manifest-path V2\Cargo.toml before starting the soak"
}

$copySettings = $null
if (-not [string]::IsNullOrWhiteSpace($SettingsPath) -and (Test-Path -LiteralPath $SettingsPath)) {
    $copySettings = Get-Content -LiteralPath $SettingsPath -Raw -Encoding utf8 | ConvertFrom-Json
}

$settingsLeaders = @()
if ($copySettings -and $copySettings.leaders) {
    $leaderIndex = 0
    foreach ($address in @($copySettings.leaders)) {
        $text = ([string]$address).Trim()
        if ([string]::IsNullOrWhiteSpace($text)) {
            continue
        }
        $leaderIndex += 1
        $settingsLeaders += "leader_$leaderIndex=$text"
    }
}

$leaderNotionalUsd = 120.0
if ($copySettings -and $copySettings.copy_ratio -and $copySettings.principal_cap_usd) {
    $ratio = [double]$copySettings.copy_ratio
    $cap = [double]$copySettings.principal_cap_usd
    if ($ratio -gt 0 -and $cap -gt 0) {
        $leaderNotionalUsd = [Math]::Max($cap * 5.0 / $ratio, 1.0)
    }
}

function Invoke-BotRound {
    param(
        [string]$ExePath,
        [string[]]$Arguments,
        [string]$StdoutPath,
        [string]$StderrPath,
        [int]$TimeoutSecs
    )

    if (Test-Path -LiteralPath $StdoutPath) {
        Remove-Item -LiteralPath $StdoutPath -Force
    }
    if (Test-Path -LiteralPath $StderrPath) {
        Remove-Item -LiteralPath $StderrPath -Force
    }

    $process = Start-Process `
        -FilePath $ExePath `
        -ArgumentList $Arguments `
        -WorkingDirectory (Get-Location) `
        -RedirectStandardOutput $StdoutPath `
        -RedirectStandardError $StderrPath `
        -WindowStyle Hidden `
        -PassThru

    if (-not $process.WaitForExit($TimeoutSecs * 1000)) {
        try {
            if ($IsWindows -or $env:OS -eq "Windows_NT") {
                $kill = Start-Process `
                    -FilePath "taskkill" `
                    -ArgumentList @("/PID", $process.Id.ToString(), "/T", "/F") `
                    -WindowStyle Hidden `
                    -Wait `
                    -PassThru
                if ($kill.ExitCode -ne 0) {
                    Stop-Process -Id $process.Id -Force -ErrorAction Stop
                }
            } else {
                $process.Kill()
            }
        } catch {
            Write-SoakLog "round child timeout kill failed pid=$($process.Id) error=$($_.Exception.Message)"
        }
        return 124
    }

    $process.Refresh()
    if ($null -eq $process.ExitCode) {
        Start-Sleep -Milliseconds 200
        $process.Refresh()
    }
    if ($null -eq $process.ExitCode) {
        Write-SoakLog "round child exited but ExitCode was unavailable pid=$($process.Id); treating as success only if report validation passes"
        return 0
    }
    return $process.ExitCode
}

$leaderArgs = @()
if ($settingsLeaders.Count -gt 0) {
    foreach ($leader in $settingsLeaders) {
        $leaderArgs += @("--leader", $leader)
    }
} elseif ($LeaderSet -eq "all") {
    $leaderArgs += @(
        "--leader", "scalp_1=0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
        "--leader", "scalp_2=0x6ac0b46b32dc429dbd129a503292f88649d2b8a0",
        "--leader", "scalp_3=0x117a7c349b953d54154312d97a20c9a2769adbd4"
    )
    $leaderArgs += @(
        "--leader", "swing_1=0x9dead8fffcbf130e7658f672d2c081d91178d617",
        "--leader", "swing_2=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0"
    )
} else {
    $leaderArgs += @(
        "--leader", "swing_1=0x9dead8fffcbf130e7658f672d2c081d91178d617",
        "--leader", "swing_2=0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0"
    )
}
Write-SoakLog "copy settings leaders=$($leaderArgs.Count / 2) leader_notional_usd=$leaderNotionalUsd"

$round = 0
$consecutiveDegradedWatcherRounds = 0
while ($true) {
    $round += 1
    if ($MaxRounds -gt 0 -and $round -gt $MaxRounds) {
        Write-SoakLog "completed max_rounds=$MaxRounds"
        Stop-WithNotification -Code 0 -Status "completed" -Reason "max_rounds" -Detail "completed max_rounds=$MaxRounds"
    }

    $roundTag = "{0:D4}" -f $round
    $reportPath = "$prefix-round-$roundTag.json"
    $stderrPath = "$prefix-round-$roundTag.err.log"
    Write-SoakLog "round=$round starting report=$reportPath"

    $botArgs = @(
        "copy-live-daemon-supervisor",
        "--config", "V2\config\local.toml"
    ) + $leaderArgs + @(
        "--account-id", "addr_a",
        "--coin", "xyz:XYZ100",
        "--side", "buy",
        "--persistence", $persistencePath,
        "--shadow-history", $shadowPath,
        "--leader-notional-usd", "$leaderNotionalUsd",
        "--leader-size", "1",
        "--duration-secs", "$WindowSecs",
        "--max-events", "$MaxEvents",
        "--max-live-orders", "1",
        "--max-total-notional-usd", "$MaxTotalNotionalUsd",
        "--max-total-fees-usd", "$MaxTotalFeesUsd",
        "--max-slippage-bps", "50",
        "--cleanup-max-slippage-bps", "50",
        "--hold-positions-after-submit", $holdPositionsArg,
        "--live-gate", "true",
        "--allow-live-submit", "true",
        "--confirm-mainnet-live", "true",
        "--submit", "true"
    )
    $roundTimeoutSecs = $WindowSecs + $RoundTimeoutBufferSecs
    $roundExitCode = Invoke-BotRound `
        -ExePath $botExe `
        -Arguments $botArgs `
        -StdoutPath $reportPath `
        -StderrPath $stderrPath `
        -TimeoutSecs $roundTimeoutSecs

    if ($roundExitCode -eq 124) {
        Write-SoakLog "round=$round timed out after ${roundTimeoutSecs}s; child process was killed, stderr=$stderrPath"
        Stop-WithNotification -Code 124 -Status "stopped" -Reason "round_timeout" -Detail "round=$round timeout_secs=$roundTimeoutSecs stderr=$stderrPath"
    }

    if ($roundExitCode -ne 0) {
        Write-SoakLog "round=$round failed exit_code=$roundExitCode stderr=$stderrPath"
        Stop-WithNotification -Code $roundExitCode -Status "failed" -Reason "round_exit_code" -Detail "round=$round exit_code=$roundExitCode stderr=$stderrPath"
    }

    $report = Get-Content -LiteralPath $reportPath -Raw -Encoding utf8 | ConvertFrom-Json
    $submittedCount = @($report.persistent_live_submit.submitted_reports).Count
    $evidenceCount = @($report.persistent_live_submit.order_evidence).Count
    $cleanupCount = @($report.persistent_live_submit.cleanup_runbooks).Count
    $cleanupErrors = @($report.persistent_live_submit.cleanup_errors).Count
    $finalReconcileHealth = $false
    foreach ($check in @($report.checks)) {
        if ($check.name -eq "final_reconcile_health") {
            $finalReconcileHealth = [bool]$check.ok
            break
        }
    }
    $finalFlat = $true
    foreach ($reconcile in @($report.final_reconciliations)) {
        if (-not $reconcile.ok) {
            $finalFlat = $false
        }
    }
    $failedChecks = @($report.checks | Where-Object { -not $_.ok } | ForEach-Object {
        "$($_.name): $($_.detail)"
    })
    $failedCheckNames = @($report.checks | Where-Object { -not $_.ok } | ForEach-Object {
        [string]$_.name
    })
    $watcherOnlyDegraded = (-not [bool]$report.ok) `
        -and ([int]$report.events_received -eq 0) `
        -and ($submittedCount -eq 0) `
        -and ($cleanupErrors -eq 0) `
        -and $finalReconcileHealth `
        -and ($failedCheckNames.Count -gt 0) `
        -and (@($failedCheckNames | Where-Object { $_ -notin @("watcher_runtime", "watcher_progress") }).Count -eq 0)
    $reconcileOnlyDegraded = (-not [bool]$report.ok) `
        -and ($submittedCount -eq 0) `
        -and ($evidenceCount -eq 0) `
        -and ($cleanupErrors -eq 0) `
        -and ($failedCheckNames.Count -gt 0) `
        -and (@($failedCheckNames | Where-Object { $_ -notin @("exchange_submit_mode", "final_reconcile_health") }).Count -eq 0) `
        -and (@($report.final_reconciliations).Count -gt 0) `
        -and (@($report.final_reconciliations | Where-Object { -not $_.error }).Count -eq 0)
    if ($watcherOnlyDegraded) {
        $consecutiveDegradedWatcherRounds += 1
    } elseif ($reconcileOnlyDegraded) {
        $consecutiveDegradedWatcherRounds += 1
    } else {
        $consecutiveDegradedWatcherRounds = 0
    }
    $summary = [ordered]@{
        run_id = $runId
        round = $round
        ok = [bool]$report.ok
        ready_for_unattended_submit = [bool]$report.submit_evidence_contract.ready_for_unattended_submit
        submitted_reports = $submittedCount
        order_evidence = $evidenceCount
        cleanup_runbooks = $cleanupCount
        cleanup_errors = $cleanupErrors
        executable_submit_plan_refs = @($report.executable_submit_plan_refs).Count
        suppressed_submit_plan_refs = @($report.suppressed_submit_plan_refs).Count
        shadow_records_written = [int]$report.shadow_records_written
        events_received = [int]$report.events_received
        watcher_status = [string]$report.watcher_status
        final_flat = [bool]$finalFlat
        final_reconcile_health = [bool]$finalReconcileHealth
        hold_positions_after_submit = [bool]$report.hold_positions_after_submit
        watcher_only_degraded = [bool]$watcherOnlyDegraded
        reconcile_only_degraded = [bool]$reconcileOnlyDegraded
        consecutive_degraded_watcher_rounds = [int]$consecutiveDegradedWatcherRounds
        failed_checks = $failedChecks
        report_path = $reportPath
        timestamp = (Get-Date -Format o)
    }
    $summaryLine = $summary | ConvertTo-Json -Compress
    [System.IO.File]::AppendAllText($summaryPath, "$summaryLine`n", [System.Text.UTF8Encoding]::new($false))
    Write-SoakLog "round=$round ok=$($summary.ok) submitted=$submittedCount evidence=$evidenceCount cleanup_errors=$cleanupErrors final_reconcile_health=$finalReconcileHealth hold_positions_after_submit=$($summary.hold_positions_after_submit) ready=$($summary.ready_for_unattended_submit)"
    if ($failedChecks.Count -gt 0) {
        Write-SoakLog "round=$round failed_checks=$($failedChecks -join ' | ')"
    }

    if ($watcherOnlyDegraded) {
        Write-SoakLog "round=$round watcher degraded before events; keeping soak alive after ${DegradedSleepSecs}s backoff consecutive_degraded_watcher_rounds=$consecutiveDegradedWatcherRounds"
        Start-Sleep -Seconds $DegradedSleepSecs
        continue
    }

    if ($reconcileOnlyDegraded) {
        Write-SoakLog "round=$round reconcile degraded after read-only final check; keeping soak alive after ${DegradedSleepSecs}s backoff consecutive_degraded_rounds=$consecutiveDegradedWatcherRounds"
        Start-Sleep -Seconds $DegradedSleepSecs
        continue
    }

    if (-not $report.ok -or (-not $HoldPositionsAfterSubmit -and -not $finalReconcileHealth) -or $cleanupErrors -gt 0) {
        Write-SoakLog "round=$round stopping because health check failed"
        Stop-WithNotification -Code 2 -Status "failed" -Reason "health_check_failed" -Detail "round=$round failed_checks=$($failedChecks -join ' | ')"
    }

    if ($StopAfterRealSubmit -and $submittedCount -gt 0) {
        Write-SoakLog "round=$round stopping after first real submit evidence"
        Stop-WithNotification -Code 0 -Status "completed" -Reason "stop_after_real_submit" -Detail "round=$round submitted=$submittedCount evidence=$evidenceCount"
    }

    Start-Sleep -Seconds $SleepSecs
}
