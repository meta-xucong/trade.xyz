param(
    [string]$BaseUrl = "http://127.0.0.1:18845",
    [int]$PollSecs = 60,
    [int]$WindowSecs = 600,
    [int]$StaleRoundGraceSecs = 1200,
    [double]$PrincipalCapUsd = 35.0,
    [double]$Leverage = 10.0,
    [double]$MaxTotalNotionalUsd = 0.0,
    [double]$MaxTotalFeesUsd = 1.0,
    [int]$StopConfirmPolls = 3,
    [int]$NotificationCooldownSecs = 900,
    [string]$AccountId = "addr_a",
    [string[]]$AccountIds = @(),
    [string[]]$Leaders = @(
        "0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
        "0x6ac0b46b32dc429dbd129a503292f88649d2b8a0",
        "0x117a7c349b953d54154312d97a20c9a2769adbd4",
        "0x9dead8fffcbf130e7658f672d2c081d91178d617",
        "0xd8c5228c515db3043dfa0c8cd6f22450ee9a99b0"
    ),
    [string[]]$Markets = @("xyz_perp", "hl_perp", "cash_perp", "spot"),
    [double]$CopyRatio = 0.2,
    [string]$SettingsPath = ".codex-longrun\copy-ui-settings.json",
    [string]$PersistencePath = ".codex-longrun\persistent-live-soak-resume-current-snapshot.json",
    [string]$LogPath = ".codex-longrun\copy-health-monitor.log",
    [string]$VaultPasswordEnv = "TRADE_XYZ_VAULT_PASSWORD",
    [string]$BotExePath = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $projectRoot
New-Item -ItemType Directory -Force -Path ".codex-longrun" | Out-Null
$notificationSettingsPath = ".codex-longrun\notification-settings.json"
$stopNotificationStatePath = ".codex-longrun\copy-stop-notification-state.json"
$copyLiveSoakPausePath = ".codex-longrun\copy-live-soak-paused.flag"
$stopCandidateReason = ""
$stopCandidateCount = 0
$lastStopNotificationReason = ""
$lastStopNotificationAt = [datetime]::MinValue
function Write-MonitorLog {
    param([string]$Message)
    $line = "$(Get-Date -Format o) $Message"
    Add-Content -LiteralPath $LogPath -Value $line -Encoding utf8
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
        Write-MonitorLog "notification settings parse failed: $($_.Exception.Message)"
        return $null
    }
}

function Send-MonitorNotification {
    param(
        [string]$Status,
        [string]$Reason,
        [string]$Detail
    )
    $settings = Get-NotificationSettings
    if ($null -eq $settings) {
        return $false
    }
    $title = "trade.xyz copy health monitor $Status"
    $message = @(
        "status=$Status",
        "reason=$Reason",
        "detail=$Detail",
        "time=$(Get-Date -Format o)"
    ) -join "`n"
    try {
        if ([string]$settings.provider -eq "feishu") {
            $webhook = [string]$settings.feishu_webhook
            if ([string]::IsNullOrWhiteSpace($webhook)) {
                return $false
            }
            $body = @{
                msg_type = "text"
                content = @{ text = "$title`n$message" }
            } | ConvertTo-Json -Compress -Depth 6
            Invoke-RestMethod -Uri $webhook -Method POST -ContentType "application/json" -Body $body -TimeoutSec 10 | Out-Null
            Write-MonitorLog "notification sent provider=feishu status=$Status reason=$Reason"
            return $true
        } else {
            $sendKey = [string]$settings.serverchan_sendkey
            if ([string]::IsNullOrWhiteSpace($sendKey)) {
                return $false
            }
            Invoke-RestMethod -Uri "https://sctapi.ftqq.com/$sendKey.send" -Method POST -ContentType "application/x-www-form-urlencoded" -Body @{
                title = $title
                desp = $message
                short = $Reason
                noip = "1"
            } -TimeoutSec 10 | Out-Null
            Write-MonitorLog "notification sent provider=serverchan status=$Status reason=$Reason"
            return $true
        }
    } catch {
        $errorMessage = [string]$_.Exception.Message
        if ($null -ne $settings.serverchan_sendkey) {
            $errorMessage = $errorMessage.Replace([string]$settings.serverchan_sendkey, "***")
        }
        if ($null -ne $settings.feishu_webhook) {
            $errorMessage = $errorMessage.Replace([string]$settings.feishu_webhook, "***")
        }
        Write-MonitorLog "notification failed provider=$($settings.provider) status=$Status error=$errorMessage"
        return $false
    }
}

function Get-DiagnosticRunId {
    param([string]$Diagnostic)
    if ([string]::IsNullOrWhiteSpace($Diagnostic)) {
        return ""
    }
    $match = [regex]::Match($Diagnostic, "run_id=([0-9]{8}-[0-9]{6})")
    if ($match.Success) {
        return $match.Groups[1].Value
    }
    return ""
}

function Read-StopNotificationState {
    if (-not (Test-Path -LiteralPath $stopNotificationStatePath)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $stopNotificationStatePath -Raw -Encoding utf8 | ConvertFrom-Json
    } catch {
        Write-MonitorLog "stop notification state parse failed: $($_.Exception.Message)"
        return $null
    }
}

function Write-StopNotificationState {
    param(
        [string]$Provider,
        [string]$Reason,
        [string]$Diagnostic
    )
    try {
        $state = [ordered]@{
            source = "monitor"
            provider = $Provider
            run_id = Get-DiagnosticRunId $Diagnostic
            status = "stopped"
            reason = $Reason
            detail = $Diagnostic
            sent_at = (Get-Date -Format o)
        }
        $json = $state | ConvertTo-Json -Compress -Depth 6
        Set-Content -LiteralPath $stopNotificationStatePath -Value $json -Encoding utf8
    } catch {
        Write-MonitorLog "stop notification state write failed: $($_.Exception.Message)"
    }
}

function Test-RecentStopNotificationAlreadySent {
    param(
        [string]$Reason,
        [string]$Diagnostic
    )
    $state = Read-StopNotificationState
    if ($null -eq $state -or $null -eq $state.sent_at) {
        return $false
    }
    try {
        $sentAt = [datetime]::Parse([string]$state.sent_at)
    } catch {
        return $false
    }
    $ageSecs = ((Get-Date) - $sentAt).TotalSeconds
    if ($ageSecs -ge $NotificationCooldownSecs) {
        return $false
    }
    $stateSource = [string]$state.source
    $stateStatus = [string]$state.status
    if ($stateSource -eq "runner" -and $stateStatus -in @("failed", "stopped")) {
        Write-MonitorLog "recent runner stop notification already covers monitor stop reason=$Reason prior_reason=$($state.reason) age_secs=$([Math]::Round($ageSecs, 1))"
        return $true
    }
    $diagnosticRunId = Get-DiagnosticRunId $Diagnostic
    $stateRunId = [string]$state.run_id
    if (-not [string]::IsNullOrWhiteSpace($diagnosticRunId) -and $stateRunId -eq $diagnosticRunId) {
        return $true
    }
    return ([string]$state.reason) -eq $Reason
}

function Invoke-Json {
    param(
        [string]$Path,
        [string]$Method = "GET",
        [object]$Body = $null
    )
    $uri = "$BaseUrl$Path"
    if ($null -eq $Body) {
        return Invoke-RestMethod -Uri $uri -Method $Method -TimeoutSec 10
    }
    return Invoke-RestMethod -Uri $uri -Method $Method -ContentType "application/json" -Body ($Body | ConvertTo-Json -Compress -Depth 8) -TimeoutSec 15
}

function Convert-ToDoubleOrZero {
    param([object]$Value)
    $parsed = 0.0
    if ([double]::TryParse([string]$Value, [ref]$parsed)) {
        return $parsed
    }
    return 0.0
}

function Get-PositionTruthRiskDiagnostic {
    try {
        $state = Invoke-Json "/api/state"
    } catch {
        return ""
    }
    $payload = $state
    if ($state.PSObject.Properties.Name -contains "ok") {
        if (-not [bool]$state.ok) {
            return ""
        }
        if (($state.PSObject.Properties.Name -contains "data") -and $null -ne $state.data) {
            $payload = $state.data
        }
    }
    if ($null -eq $payload -or $null -eq $payload.position_truth) {
        return ""
    }
    $truth = $payload.position_truth
    $unattributedValue = Convert-ToDoubleOrZero $truth.unattributed_position_value_usd
    if ($unattributedValue + 1e-9 -lt 10.0) {
        return ""
    }
    $positions = @($payload.positions | Where-Object {
        ([string]$_.owner) -eq "unattributed" -and
        ((Convert-ToDoubleOrZero $_.position_value_usd) + 1e-9 -ge 10.0)
    } | ForEach-Object {
        "$($_.account_id):$($_.coin):size=$($_.size):value=$($_.position_value_usd):pnl=$($_.pnl_usd)"
    })
    $positionText = if ($positions.Count -gt 0) { $positions -join "," } else { "none" }
    return "unattributed_position_value_usd=$unattributedValue count=$($truth.unattributed_position_count) positions=$positionText"
}

function Get-ResolvedBotExePath {
    $resolvedBotExePath = $BotExePath
    if ([string]::IsNullOrWhiteSpace($resolvedBotExePath)) {
        $resolvedBotExePath = $env:TRADE_XYZ_BOT_EXE
    }
    return $resolvedBotExePath
}

function Invoke-UnattributedLedgerRecovery {
    param([string]$Diagnostic)
    $resolvedBotExePath = Get-ResolvedBotExePath
    if ([string]::IsNullOrWhiteSpace($resolvedBotExePath) -or -not (Test-Path -LiteralPath $resolvedBotExePath)) {
        Write-MonitorLog "unattributed recovery skipped; bot exe unavailable path=$resolvedBotExePath"
        return $false
    }
    $status = $null
    try {
        $statusResponse = Invoke-Json "/api/copy/live-soak/status"
        if ($statusResponse.ok) {
            $status = $statusResponse.data
        }
    } catch {
        Write-MonitorLog "unattributed recovery status lookup failed: $($_.Exception.Message)"
    }
    $runId = if ($null -ne $status -and -not [string]::IsNullOrWhiteSpace([string]$status.run_id)) {
        [string]$status.run_id
    } else {
        Get-Date -Format "yyyyMMdd-HHmmss"
    }
    $shadowPath = ".codex-longrun\persistent-live-soak-$runId-shadow.jsonl"
    if ($null -ne $status -and -not [string]::IsNullOrWhiteSpace([string]$status.latest_log_path)) {
        $candidate = [string]$status.latest_log_path
        if ($candidate -match "persistent-live-soak-(.+?)-run\.log$") {
            $shadowPath = ".codex-longrun\persistent-live-soak-$($Matches[1])-shadow.jsonl"
        }
    }

    $runtimeAccountIds = Get-RuntimeAccountIds
    $runtimeMarkets = Get-RuntimeMarkets
    $args = @("copy-live-daemon-supervisor", "--config", "V2\config\local.toml")
    for ($index = 0; $index -lt $Leaders.Count; $index++) {
        $leaderAddress = [string]$Leaders[$index]
        if (-not [string]::IsNullOrWhiteSpace($leaderAddress)) {
            $args += @("--leader", "leader_$($index + 1)=$leaderAddress")
        }
    }
    foreach ($market in $runtimeMarkets) {
        $args += @("--market", $market)
    }
    foreach ($accountId in $runtimeAccountIds) {
        $args += @("--account-id", $accountId)
    }
    $args += @(
        "--side", "buy",
        "--persistence", $PersistencePath,
        "--shadow-history", $shadowPath,
        "--leader-notional-usd", "1750",
        "--leader-size", "1",
        "--duration-secs", "2",
        "--max-events", "1",
        "--max-live-orders", "2",
        "--max-total-notional-usd", [string]$MaxTotalNotionalUsd,
        "--max-total-fees-usd", [string]$MaxTotalFeesUsd,
        "--max-slippage-bps", "50",
        "--cleanup-max-slippage-bps", "50",
        "--hold-positions-after-submit", "true",
        "--live-gate", "true",
        "--allow-live-submit", "true",
        "--confirm-mainnet-live", "true",
        "--submit", "false"
    )
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $recoveryReportPath = ".codex-longrun\copy-unattributed-ledger-recovery-$stamp.json"
    Write-MonitorLog "unattributed recovery starting run_id=$runId shadow=$shadowPath report=$recoveryReportPath diagnostic=$Diagnostic"
    try {
        $output = & $resolvedBotExePath @args 2>&1 | Out-String
        Set-Content -LiteralPath $recoveryReportPath -Value $output -Encoding utf8
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0) {
            Write-MonitorLog "unattributed recovery failed exit_code=$exitCode report=$recoveryReportPath"
            return $false
        }
    } catch {
        Write-MonitorLog "unattributed recovery command failed: $($_.Exception.Message)"
        return $false
    }
    $remainingRisk = Get-PositionTruthRiskDiagnostic
    if ([string]::IsNullOrWhiteSpace($remainingRisk)) {
        Write-MonitorLog "unattributed recovery succeeded; position truth risk cleared report=$recoveryReportPath"
        return $true
    }
    Write-MonitorLog "unattributed recovery did not clear risk; remaining=$remainingRisk report=$recoveryReportPath"
    return $false
}

function Get-LatestRunDiagnostic {
    try {
        $status = Invoke-Json "/api/copy/live-soak/status"
    } catch {
        return "frontend status unavailable: $($_.Exception.Message)"
    }
    if (-not $status.ok) {
        return "frontend status error: $($status.error)"
    }
    $data = $status.data
    $parts = @("running=$($data.running)", "run_id=$($data.run_id)", "message=$($data.message)")
    if ($data.latest_report_path -and (Test-Path -LiteralPath $data.latest_report_path)) {
        try {
            $report = Read-JsonObjectFile -Path $data.latest_report_path
            $failed = @($report.checks | Where-Object { -not $_.ok } | ForEach-Object { "$($_.name): $($_.detail)" })
            $parts += "report_ok=$($report.ok)"
            $parts += "watcher_status=$($report.watcher_status)"
            $parts += "events=$($report.events_received)"
            $submittedReports = @($report.persistent_live_submit.submitted_reports)
            $submittedCount = @($submittedReports | Where-Object {
                $kind = [string]$_.kind
                $dryRun = $false
                if ($null -ne $_.dry_run) {
                    $dryRun = [bool]$_.dry_run
                }
                $kind -eq "submitted" -and -not $dryRun
            }).Count
            $preSubmitSkippedCount = @($submittedReports | Where-Object {
                $kind = [string]$_.kind
                $message = [string]$_.message
                $kind -eq "error" -and $message.ToLowerInvariant().Contains("copy submit skipped before exchange")
            }).Count
            $parts += "submitted=$submittedCount"
            if ($preSubmitSkippedCount -gt 0) {
                $parts += "pre_submit_skipped=$preSubmitSkippedCount"
            }
            $parts += "evidence=$(@($report.persistent_live_submit.order_evidence).Count)"
            if ($failed.Count -gt 0) {
                $parts += "failed_checks=$($failed -join ' | ')"
            }
        } catch {
            $parts += "report_parse_error=$($_.Exception.Message)"
        }
    }
    return ($parts -join "; ")
}

function Read-JsonObjectFile {
    param([string]$Path)
    $raw = Get-Content -LiteralPath $Path -Raw -Encoding utf8
    if ([string]::IsNullOrWhiteSpace($raw)) {
        throw "empty JSON report at $Path"
    }
    try {
        $parsed = $raw | ConvertFrom-Json
        if ($null -eq $parsed) {
            throw "JSON report parsed to null"
        }
        return $parsed
    } catch {
        $start = $raw.IndexOf('{')
        $end = $raw.LastIndexOf('}')
        if ($start -ge 0 -and $end -gt $start) {
            $json = $raw.Substring($start, $end - $start + 1)
            try {
                $parsed = $json | ConvertFrom-Json
                if ($null -eq $parsed) {
                    throw "JSON object parsed to null"
                }
                return $parsed
            } catch {
                throw "failed to parse JSON object from $Path after stripping noisy output: $($_.Exception.Message)"
            }
        }
        throw "failed to parse JSON object from ${Path}: $($_.Exception.Message)"
    }
}

function Get-SoakProcess {
    $pidPath = ".codex-longrun\persistent-live-soak-detached-current.pid"
    if (Test-Path -LiteralPath $pidPath) {
        $pidText = Get-Content -LiteralPath $pidPath -Raw -ErrorAction SilentlyContinue
        $soakPid = 0
        if ([int]::TryParse($pidText.Trim(), [ref]$soakPid)) {
            $byPid = Get-CimInstance Win32_Process -Filter "ProcessId=$soakPid" -ErrorAction SilentlyContinue
            if ($byPid -and (Test-SoakProcessCommand $byPid)) {
                return $byPid
            }
        }
    }
    return Get-CimInstance Win32_Process | Where-Object {
        Test-SoakProcessCommand $_
    } | Sort-Object CreationDate -Descending | Select-Object -First 1
}

function Test-SoakProcessCommand {
    param([object]$Process)
    if ($null -eq $Process) {
        return $false
    }
    $commandLine = [string]$Process.CommandLine
    $isWrapper = Test-SoakWrapperCommand $Process
    $isRoundChild = $Process.Name -eq "trade_xyz_bot_v2.exe" `
        -and $commandLine -like "*copy-live-daemon-supervisor*"
    return $isWrapper -or $isRoundChild
}

function Reset-StopCandidate {
    param([string]$Context)
    if ($script:stopCandidateCount -gt 0) {
        Write-MonitorLog "stop candidate cleared context=$Context prior_reason=$script:stopCandidateReason prior_count=$script:stopCandidateCount"
    }
    $script:stopCandidateReason = ""
    $script:stopCandidateCount = 0
}

function Test-StopCandidateConfirmed {
    param(
        [string]$Reason,
        [string]$Diagnostic
    )
    if ($script:stopCandidateReason -ne $Reason) {
        $script:stopCandidateReason = $Reason
        $script:stopCandidateCount = 0
    }
    $script:stopCandidateCount += 1
    if ($script:stopCandidateCount -lt [Math]::Max(1, $StopConfirmPolls)) {
        Write-MonitorLog "stop candidate pending reason=$Reason count=$script:stopCandidateCount/$StopConfirmPolls diagnostic=$Diagnostic"
        return $false
    }
    Write-MonitorLog "stop candidate confirmed reason=$Reason count=$script:stopCandidateCount diagnostic=$Diagnostic"
    return $true
}

function Send-ConfirmedStopNotification {
    param(
        [string]$Reason,
        [string]$Diagnostic
    )
    $now = Get-Date
    $cooldownActive = $script:lastStopNotificationReason -eq $Reason -and
        (($now - $script:lastStopNotificationAt).TotalSeconds -lt $NotificationCooldownSecs)
    if ($cooldownActive) {
        $remaining = [Math]::Round($NotificationCooldownSecs - ($now - $script:lastStopNotificationAt).TotalSeconds, 1)
        Write-MonitorLog "notification suppressed by cooldown reason=$Reason remaining_secs=$remaining"
        return
    }
    if (Test-RecentStopNotificationAlreadySent -Reason $Reason -Diagnostic $Diagnostic) {
        Write-MonitorLog "notification suppressed by shared state reason=$Reason diagnostic=$Diagnostic"
        return
    }
    $sent = Send-MonitorNotification -Status "stopped" -Reason $Reason -Detail $Diagnostic
    if ($sent) {
        $settings = Get-NotificationSettings
        $provider = if ($null -ne $settings -and -not [string]::IsNullOrWhiteSpace([string]$settings.provider)) {
            [string]$settings.provider
        } else {
            ""
        }
        Write-StopNotificationState -Provider $provider -Reason $Reason -Diagnostic $Diagnostic
    }
    $script:lastStopNotificationReason = $Reason
    $script:lastStopNotificationAt = $now
}

function Test-SoakWrapperCommand {
    param([object]$Process)
    if ($null -eq $Process) {
        return $false
    }
    $commandLine = [string]$Process.CommandLine
    return $Process.Name -match "^(powershell|pwsh)\.exe$" `
        -and $commandLine -like "*run-persistent-live-soak.ps1*" `
        -and $commandLine -match "(?i)(^|\\s)-File\\s+" `
        -and $commandLine -notlike "*Get-CimInstance*"
}

function Test-SoakProcessRunning {
    return $null -ne (Get-SoakProcess)
}

function Get-SoakProcesses {
    return Get-CimInstance Win32_Process | Where-Object {
        Test-SoakProcessCommand $_
    } | Sort-Object CreationDate -Descending
}

function Get-LatestSoakRunLog {
    return Get-ChildItem -LiteralPath ".codex-longrun" -Filter "persistent-live-soak-*-run.log" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
}

function Test-SoakHeartbeatStale {
    $log = Get-LatestSoakRunLog
    if ($null -eq $log) {
        return $false
    }
    $ageSecs = ((Get-Date) - $log.LastWriteTime).TotalSeconds
    $limitSecs = [Math]::Max($WindowSecs + $StaleRoundGraceSecs, $PollSecs * 3)
    return $ageSecs -gt $limitSecs
}

function Get-SoakHeartbeatDiagnostic {
    $log = Get-LatestSoakRunLog
    if ($null -eq $log) {
        return "no persistent live soak run log found"
    }
    $ageSecs = [Math]::Round(((Get-Date) - $log.LastWriteTime).TotalSeconds, 1)
    $lastLine = ""
    try {
        $lastLine = (Get-Content -LiteralPath $log.FullName -Tail 1 -Encoding utf8)
    } catch {
        $lastLine = "failed to read run log tail: $($_.Exception.Message)"
    }
    return "run_log=$($log.Name); age_secs=$ageSecs; stale_limit_secs=$([Math]::Max($WindowSecs + $StaleRoundGraceSecs, $PollSecs * 3)); last=$lastLine"
}

function Ensure-VaultUnlocked {
    $status = Invoke-Json "/api/vault/status"
    if ($status.ok -and $status.data.unlocked) {
        return
    }
    $password = [Environment]::GetEnvironmentVariable($VaultPasswordEnv)
    if ([string]::IsNullOrWhiteSpace($password)) {
        throw "Vault is locked and environment variable $VaultPasswordEnv is not set for the monitor process"
    }
    $unlock = Invoke-Json "/api/vault/unlock" "POST" @{ password = $password }
    if (-not $unlock.ok -or -not $unlock.data.unlocked) {
        throw "Vault unlock failed: $($unlock.error)"
    }
}

function Get-CopyUiSettings {
    if ([string]::IsNullOrWhiteSpace($SettingsPath) -or -not (Test-Path -LiteralPath $SettingsPath)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $SettingsPath -Raw -Encoding utf8 | ConvertFrom-Json
    } catch {
        Write-MonitorLog "copy settings parse failed path=$SettingsPath error=$($_.Exception.Message)"
        return $null
    }
}

function Get-RuntimeAccountIds {
    $runtimeAccountIds = @()
    if ($AccountIds.Count -gt 0) {
        foreach ($accountId in $AccountIds) {
            $text = ([string]$accountId).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text) -and $runtimeAccountIds -notcontains $text) {
                $runtimeAccountIds += $text
            }
        }
    }
    if ($runtimeAccountIds.Count -eq 0) {
        $copySettings = Get-CopyUiSettings
        if ($copySettings -and $copySettings.account_ids) {
            foreach ($accountId in @($copySettings.account_ids)) {
                $text = ([string]$accountId).Trim()
                if (-not [string]::IsNullOrWhiteSpace($text) -and $runtimeAccountIds -notcontains $text) {
                    $runtimeAccountIds += $text
                }
            }
        }
        if ($runtimeAccountIds.Count -eq 0 -and $copySettings -and $copySettings.account_id) {
            $text = ([string]$copySettings.account_id).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                $runtimeAccountIds += $text
            }
        }
    }
    if ($runtimeAccountIds.Count -eq 0) {
        $runtimeAccountIds = @($AccountId)
    }
    return $runtimeAccountIds
}

function Get-RuntimeLeaders {
    $copySettings = Get-CopyUiSettings
    $runtimeLeaders = @()
    if ($copySettings -and $copySettings.leaders) {
        foreach ($leader in @($copySettings.leaders)) {
            $text = ([string]$leader).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text) -and $runtimeLeaders -notcontains $text) {
                $runtimeLeaders += $text
            }
        }
    }
    if ($runtimeLeaders.Count -eq 0) {
        foreach ($leader in $Leaders) {
            $text = ([string]$leader).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text) -and $runtimeLeaders -notcontains $text) {
                $runtimeLeaders += $text
            }
        }
    }
    return $runtimeLeaders
}

function Get-RuntimeMarkets {
    $copySettings = Get-CopyUiSettings
    $runtimeMarkets = @()
    if ($copySettings -and $copySettings.markets) {
        foreach ($market in @($copySettings.markets)) {
            $text = ([string]$market).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text) -and $runtimeMarkets -notcontains $text) {
                $runtimeMarkets += $text
            }
        }
    }
    if ($runtimeMarkets.Count -eq 0) {
        foreach ($market in $Markets) {
            $text = ([string]$market).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text) -and $runtimeMarkets -notcontains $text) {
                $runtimeMarkets += $text
            }
        }
    }
    if ($runtimeMarkets.Count -eq 0) {
        $runtimeMarkets = @("xyz_perp", "hl_perp", "cash_perp", "spot")
    }
    return $runtimeMarkets
}

function Set-CopyRuntimeSettings {
    $runtimeAccountIds = Get-RuntimeAccountIds
    $runtimeLeaders = Get-RuntimeLeaders
    $runtimeMarkets = Get-RuntimeMarkets
    $settings = Invoke-Json "/api/copy/settings" "POST" @{
        leaders = $runtimeLeaders
        markets = $runtimeMarkets
        copy_ratio = $CopyRatio
        principal_cap_usd = $PrincipalCapUsd
        leverage = $Leverage
        account_id = $runtimeAccountIds[0]
        account_ids = $runtimeAccountIds
    }
    if (-not $settings.ok) {
        throw "copy settings update failed: $($settings.error)"
    }
    $notionalCap = $PrincipalCapUsd * $Leverage
    $manual = Invoke-Json "/api/manual-settings" "POST" @{
        max_manual_order_notional_usd = $notionalCap
        account_max_order_notional_usd = $notionalCap
        account_ids = $runtimeAccountIds
    }
    if (-not $manual.ok) {
        throw "manual settings update failed: $($manual.error)"
    }
}

function Set-LatestSoakPid {
    param([datetime]$StartedAt)
    $deadline = (Get-Date).AddSeconds(12)
    $proc = $null
    while ((Get-Date) -lt $deadline -and $null -eq $proc) {
        Start-Sleep -Milliseconds 750
        $proc = Get-CimInstance Win32_Process | Where-Object {
            (Test-SoakWrapperCommand $_) -and
            $_.CreationDate -ge $StartedAt.AddSeconds(-5)
        } | Sort-Object CreationDate -Descending | Select-Object -First 1
    }
    if (-not $proc) {
        $proc = Get-SoakProcess
    }
    if ($proc) {
        Set-Content -LiteralPath ".codex-longrun\persistent-live-soak-detached-current.pid" -Value ([string]$proc.ProcessId) -Encoding ascii
        return $proc.ProcessId
    }
    return $null
}

function Start-CopyLiveSoak {
    if (Test-CopyLiveSoakPaused) {
        Write-MonitorLog "restart skipped; copy live soak is paused by operator"
        return
    }
    $existing = Get-SoakProcess
    if ($existing) {
        Write-MonitorLog "restart skipped; soak process already running pid=$($existing.ProcessId)"
        return
    }
    Ensure-VaultUnlocked
    $runtimeAccountIds = Get-RuntimeAccountIds
    $runtimeMarkets = Get-RuntimeMarkets
    Set-CopyRuntimeSettings
    $startedAt = Get-Date
    $resolvedBotExePath = $BotExePath
    if ([string]::IsNullOrWhiteSpace($resolvedBotExePath)) {
        $resolvedBotExePath = $env:TRADE_XYZ_BOT_EXE
    }
    $body = @{
        window_secs = $WindowSecs
        max_rounds = 0
        max_total_notional_usd = $MaxTotalNotionalUsd
        max_total_fees_usd = $MaxTotalFeesUsd
        hold_positions_after_submit = $true
        confirm_mainnet_live = $true
        persistence_path = $PersistencePath
    }
    if (-not [string]::IsNullOrWhiteSpace($resolvedBotExePath)) {
        $body.bot_exe_path = $resolvedBotExePath
    }
    $start = Invoke-Json "/api/copy/live-soak/start" "POST" $body
    if ($start.ok) {
        $soakPid = Set-LatestSoakPid $startedAt
        if ($soakPid) {
            Write-MonitorLog "restart requested run_id=$($start.data.status.run_id) pid=$soakPid accounts=$($runtimeAccountIds -join ',') principal_cap=$PrincipalCapUsd leverage=$Leverage markets=$($runtimeMarkets -join ',') max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd"
            return
        }
        $existingAfterStart = Get-SoakProcess
        if ($existingAfterStart) {
            Set-Content -LiteralPath ".codex-longrun\persistent-live-soak-detached-current.pid" -Value ([string]$existingAfterStart.ProcessId) -Encoding ascii
            Write-MonitorLog "frontend start returned ok and an existing soak process was found before fallback pid=$($existingAfterStart.ProcessId) run_id=$($start.data.status.run_id)"
            return
        }
        Write-MonitorLog "frontend start returned ok but no soak process was detected after wait; falling back to direct soak script run_id=$($start.data.status.run_id)"
    } else {
        Write-MonitorLog "frontend start failed; falling back to direct soak script: $($start.error)"
    }
    $existingBeforeFallback = Get-SoakProcess
    if ($existingBeforeFallback) {
        Set-Content -LiteralPath ".codex-longrun\persistent-live-soak-detached-current.pid" -Value ([string]$existingBeforeFallback.ProcessId) -Encoding ascii
        Write-MonitorLog "direct soak fallback skipped; soak process appeared pid=$($existingBeforeFallback.ProcessId)"
        return
    }
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($VaultPasswordEnv))) {
        throw "direct live soak fallback is blocked because $VaultPasswordEnv is not set in the monitor process; restart through the frontend/Vault session or set the env var before using direct fallback"
    }
    $script = if (Test-Path "V2\scripts\run-persistent-live-soak.ps1") {
        "V2\scripts\run-persistent-live-soak.ps1"
    } else {
        ".codex-longrun\run-persistent-live-soak.ps1"
    }
    $args = @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        $script,
        "-WindowSecs",
        [string]$WindowSecs,
        "-MaxRounds",
        "0",
        "-MaxTotalNotionalUsd",
        [string]$MaxTotalNotionalUsd,
        "-MaxTotalFeesUsd",
        [string]$MaxTotalFeesUsd,
        "-SettingsPath",
        $SettingsPath,
        "-PersistencePath",
        $PersistencePath,
        "-HoldPositionsAfterSubmit"
    )
    if (-not [string]::IsNullOrWhiteSpace($resolvedBotExePath)) {
        $args += @("-BotExePath", $resolvedBotExePath)
    }
    $process = Start-Process -FilePath powershell -ArgumentList $args -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru
    Set-Content -LiteralPath ".codex-longrun\persistent-live-soak-detached-current.pid" -Value ([string]$process.Id) -Encoding ascii
    Write-MonitorLog "direct soak restart requested pid=$($process.Id) accounts=$($runtimeAccountIds -join ',') principal_cap=$PrincipalCapUsd leverage=$Leverage markets=$($runtimeMarkets -join ',') max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd bot_exe=$resolvedBotExePath"
}

function Stop-SoakProcessesForRestart {
    param([string]$Reason)
    $processes = @(Get-SoakProcesses)
    if ($processes.Count -eq 0) {
        Write-MonitorLog "restart cleanup found no existing soak process reason=$Reason"
        return
    }
    $ids = @($processes | ForEach-Object { $_.ProcessId })
    Write-MonitorLog "restart cleanup stopping existing soak processes reason=$Reason pids=$($ids -join ',')"
    foreach ($process in $processes) {
        try {
            Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
        } catch {
            Write-MonitorLog "restart cleanup stop failed pid=$($process.ProcessId) error=$($_.Exception.Message)"
        }
    }
    Start-Sleep -Seconds 3
    $remaining = @(Get-SoakProcesses)
    if ($remaining.Count -gt 0) {
        $remainingIds = @($remaining | ForEach-Object { $_.ProcessId })
        throw "existing soak process(es) still running after restart cleanup: $($remainingIds -join ',')"
    }
}

function Restart-CopyLiveSoakAfterStale {
    param(
        [string]$Reason,
        [string]$Diagnostic
    )
    Send-ConfirmedStopNotification -Reason $Reason -Diagnostic $Diagnostic
    Stop-SoakProcessesForRestart -Reason $Reason
    Start-CopyLiveSoak
}

function Test-CopyLiveSoakPaused {
    return Test-Path -LiteralPath $copyLiveSoakPausePath
}

$monitorAccountIds = Get-RuntimeAccountIds
$monitorMarkets = Get-RuntimeMarkets
if ([double]::IsNaN($MaxTotalNotionalUsd) -or [double]::IsInfinity($MaxTotalNotionalUsd) -or $MaxTotalNotionalUsd -le 0.0) {
    $accountCount = [Math]::Max($monitorAccountIds.Count, 1)
    $MaxTotalNotionalUsd = $PrincipalCapUsd * $Leverage * $accountCount
}
Write-MonitorLog "monitor started poll_secs=$PollSecs stop_confirm_polls=$StopConfirmPolls notification_cooldown_secs=$NotificationCooldownSecs base_url=$BaseUrl accounts=$($monitorAccountIds -join ',') principal_cap=$PrincipalCapUsd leverage=$Leverage markets=$($monitorMarkets -join ',') max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd bot_exe=$BotExePath"

while ($true) {
    try {
        if (Test-CopyLiveSoakPaused) {
            Write-MonitorLog "paused_by_operator; automatic restart disabled"
            Start-Sleep -Seconds $PollSecs
            continue
        }
        $positionTruthRisk = Get-PositionTruthRiskDiagnostic
        if (-not [string]::IsNullOrWhiteSpace($positionTruthRisk)) {
            $diagnostic = "position_truth_unattributed_live_exposure; $positionTruthRisk"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            if (Test-StopCandidateConfirmed -Reason "position_truth_unattributed" -Diagnostic $diagnostic) {
                try {
                    Stop-SoakProcessesForRestart -Reason "position_truth_unattributed_recovery"
                    if (Invoke-UnattributedLedgerRecovery -Diagnostic $diagnostic) {
                        Reset-StopCandidate "recovered_after_position_truth_risk"
                        Start-CopyLiveSoak
                    } else {
                        Send-ConfirmedStopNotification -Reason "position_truth_unattributed" -Diagnostic $diagnostic
                        Set-Content -LiteralPath $copyLiveSoakPausePath -Value "position_truth_unattributed $(Get-Date -Format o)" -Encoding ascii
                        Reset-StopCandidate "paused_after_position_truth_risk"
                    }
                } catch {
                    Send-ConfirmedStopNotification -Reason "position_truth_unattributed" -Diagnostic "$diagnostic; recovery_error=$($_.Exception.Message)"
                    Set-Content -LiteralPath $copyLiveSoakPausePath -Value "position_truth_unattributed $(Get-Date -Format o)" -Encoding ascii
                    Reset-StopCandidate "paused_after_position_truth_risk"
                }
            }
            Start-Sleep -Seconds $PollSecs
            continue
        }
        $status = Invoke-Json "/api/copy/live-soak/status"
        $soakProcess = Get-SoakProcess
        if ($status.ok -and $status.data.running -and $null -ne $soakProcess -and (Test-SoakHeartbeatStale)) {
            $diagnostic = "soak_heartbeat_stale; $(Get-SoakHeartbeatDiagnostic)"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            if (Test-StopCandidateConfirmed -Reason "soak_heartbeat_stale" -Diagnostic $diagnostic) {
                try {
                    Restart-CopyLiveSoakAfterStale -Reason "soak_heartbeat_stale" -Diagnostic $diagnostic
                    Reset-StopCandidate "restart_after_stale"
                } catch {
                    Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                    throw
                }
            }
        } elseif ($status.ok -and $status.data.running -and $null -ne $soakProcess) {
            Reset-StopCandidate "healthy"
            Write-MonitorLog "healthy pid=$($soakProcess.ProcessId) run_id=$($status.data.run_id) round=$($status.data.latest_round) message=$($status.data.message)"
        } elseif ($status.ok -and $status.data.running -and $null -eq $soakProcess) {
            $diagnostic = "frontend_running_but_soak_process_missing run_id=$($status.data.run_id) round=$($status.data.latest_round) message=$($status.data.message)"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            if (Test-StopCandidateConfirmed -Reason "soak_process_missing" -Diagnostic $diagnostic) {
                Send-ConfirmedStopNotification -Reason "soak_process_missing" -Diagnostic $diagnostic
                try {
                    Start-CopyLiveSoak
                    Reset-StopCandidate "restart_after_missing_process"
                } catch {
                    Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                    throw
                }
            }
        } elseif ((Test-SoakProcessRunning) -and (Test-SoakHeartbeatStale)) {
            $diagnostic = "soak_heartbeat_stale_without_status; $(Get-SoakHeartbeatDiagnostic)"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            if (Test-StopCandidateConfirmed -Reason "soak_heartbeat_stale" -Diagnostic $diagnostic) {
                try {
                    Restart-CopyLiveSoakAfterStale -Reason "soak_heartbeat_stale" -Diagnostic $diagnostic
                    Reset-StopCandidate "restart_after_stale_without_status"
                } catch {
                    Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                    throw
                }
            }
        } elseif (Test-SoakProcessRunning) {
            Reset-StopCandidate "healthy_fallback"
            $diagnostic = if ($status.ok) { "status_not_running message=$($status.data.message)" } else { "status_error=$($status.error)" }
            $proc = Get-SoakProcess
            Write-MonitorLog "healthy_fallback pid=$($proc.ProcessId) $diagnostic"
        } else {
            $diagnostic = Get-LatestRunDiagnostic
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            if (Test-StopCandidateConfirmed -Reason "soak_not_running" -Diagnostic $diagnostic) {
                Send-ConfirmedStopNotification -Reason "soak_not_running" -Diagnostic $diagnostic
                try {
                    Start-CopyLiveSoak
                    Reset-StopCandidate "restart_after_not_running"
                } catch {
                    Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                    throw
                }
            }
        }
    } catch {
        if (Test-SoakProcessRunning) {
            $proc = Get-SoakProcess
            if (Test-SoakHeartbeatStale) {
                $diagnostic = "status_exception=$($_.Exception.Message); $(Get-SoakHeartbeatDiagnostic)"
                Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
                if (Test-StopCandidateConfirmed -Reason "soak_heartbeat_stale" -Diagnostic $diagnostic) {
                    try {
                        Restart-CopyLiveSoakAfterStale -Reason "soak_heartbeat_stale" -Diagnostic $diagnostic
                        Reset-StopCandidate "restart_after_exception_stale"
                    } catch {
                        Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                        throw
                    }
                }
            } else {
                Reset-StopCandidate "healthy_fallback_exception"
                Write-MonitorLog "healthy_fallback pid=$($proc.ProcessId) status_exception=$($_.Exception.Message)"
            }
        } else {
            $diagnostic = "status_exception=$($_.Exception.Message); no_soak_process=true"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            if (Test-StopCandidateConfirmed -Reason "soak_not_running" -Diagnostic $diagnostic) {
                Send-ConfirmedStopNotification -Reason "soak_not_running" -Diagnostic $diagnostic
                try {
                    Start-CopyLiveSoak
                    Reset-StopCandidate "restart_after_exception_not_running"
                } catch {
                    Write-MonitorLog "monitor restart failed after status exception: $($_.Exception.Message)"
                    Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                }
            }
        }
    }
    Start-Sleep -Seconds $PollSecs
}
