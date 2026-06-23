param(
    [string]$BaseUrl = "http://127.0.0.1:18844",
    [int]$PollSecs = 60,
    [int]$WindowSecs = 600,
    [int]$StaleRoundGraceSecs = 900,
    [double]$PrincipalCapUsd = 35.0,
    [double]$Leverage = 10.0,
    [double]$MaxTotalNotionalUsd = 700.0,
    [double]$MaxTotalFeesUsd = 1.0,
    [string]$AccountId = "addr_a",
    [string[]]$Leaders = @(
        "0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
        "0x6ac0b46b32dc429dbd129a503292f88649d2b8a0"
    ),
    [string[]]$Markets = @("xyz_perp", "hl_perp", "spot"),
    [double]$CopyRatio = 0.2,
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
        return
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
                return
            }
            $body = @{
                msg_type = "text"
                content = @{ text = "$title`n$message" }
            } | ConvertTo-Json -Compress -Depth 6
            Invoke-RestMethod -Uri $webhook -Method POST -ContentType "application/json" -Body $body -TimeoutSec 10 | Out-Null
            Write-MonitorLog "notification sent provider=feishu status=$Status reason=$Reason"
        } else {
            $sendKey = [string]$settings.serverchan_sendkey
            if ([string]::IsNullOrWhiteSpace($sendKey)) {
                return
            }
            Invoke-RestMethod -Uri "https://sctapi.ftqq.com/$sendKey.send" -Method POST -ContentType "application/x-www-form-urlencoded" -Body @{
                title = $title
                desp = $message
                short = $Reason
                noip = "1"
            } -TimeoutSec 10 | Out-Null
            Write-MonitorLog "notification sent provider=serverchan status=$Status reason=$Reason"
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
    }
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
            $parts += "submitted=$(@($report.persistent_live_submit.submitted_reports).Count)"
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
    try {
        return $raw | ConvertFrom-Json
    } catch {
        $start = $raw.IndexOf('{')
        $end = $raw.LastIndexOf('}')
        if ($start -ge 0 -and $end -gt $start) {
            $json = $raw.Substring($start, $end - $start + 1)
            try {
                return $json | ConvertFrom-Json
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
    $isWrapper = $Process.Name -match "^(powershell|pwsh)\.exe$" `
        -and $commandLine -like "*run-persistent-live-soak.ps1*" `
        -and $commandLine -match "(?i)(^|\\s)-File\\s+" `
        -and $commandLine -notlike "*Get-CimInstance*"
    $isRoundChild = $Process.Name -eq "trade_xyz_bot_v2.exe" `
        -and $commandLine -like "*copy-live-daemon-supervisor*"
    return $isWrapper -or $isRoundChild
}

function Test-SoakProcessRunning {
    return $null -ne (Get-SoakProcess)
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

function Set-CopyRuntimeSettings {
    $settings = Invoke-Json "/api/copy/settings" "POST" @{
        leaders = $Leaders
        markets = $Markets
        copy_ratio = $CopyRatio
        principal_cap_usd = $PrincipalCapUsd
        leverage = $Leverage
        account_id = $AccountId
    }
    if (-not $settings.ok) {
        throw "copy settings update failed: $($settings.error)"
    }
    $notionalCap = $PrincipalCapUsd * $Leverage
    $manual = Invoke-Json "/api/manual-settings" "POST" @{
        max_manual_order_notional_usd = $notionalCap
        account_max_order_notional_usd = $notionalCap
        account_ids = @($AccountId)
    }
    if (-not $manual.ok) {
        throw "manual settings update failed: $($manual.error)"
    }
}

function Set-LatestSoakPid {
    param([datetime]$StartedAt)
    Start-Sleep -Seconds 2
    $proc = Get-CimInstance Win32_Process | Where-Object {
        (Test-SoakProcessCommand $_) -and
        $_.CreationDate -ge $StartedAt.AddSeconds(-5)
    } | Sort-Object CreationDate -Descending | Select-Object -First 1
    if ($proc) {
        Set-Content -LiteralPath ".codex-longrun\persistent-live-soak-detached-current.pid" -Value ([string]$proc.ProcessId) -Encoding ascii
        return $proc.ProcessId
    }
    return $null
}

function Start-CopyLiveSoak {
    $existing = Get-SoakProcess
    if ($existing) {
        Write-MonitorLog "restart skipped; soak process already running pid=$($existing.ProcessId)"
        return
    }
    Ensure-VaultUnlocked
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
        Write-MonitorLog "restart requested run_id=$($start.data.status.run_id) pid=$soakPid principal_cap=$PrincipalCapUsd leverage=$Leverage markets=$($Markets -join ',') max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd"
        return
    }
    Write-MonitorLog "frontend start failed; falling back to direct soak script: $($start.error)"
    $script = if (Test-Path ".codex-longrun\run-persistent-live-soak.ps1") {
        ".codex-longrun\run-persistent-live-soak.ps1"
    } else {
        "V2\scripts\run-persistent-live-soak.ps1"
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
        ".codex-longrun/copy-ui-settings.json",
        "-PersistencePath",
        $PersistencePath,
        "-HoldPositionsAfterSubmit"
    )
    if (-not [string]::IsNullOrWhiteSpace($resolvedBotExePath)) {
        $args += @("-BotExePath", $resolvedBotExePath)
    }
    $process = Start-Process -FilePath powershell -ArgumentList $args -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru
    Set-Content -LiteralPath ".codex-longrun\persistent-live-soak-detached-current.pid" -Value ([string]$process.Id) -Encoding ascii
    Write-MonitorLog "direct soak restart requested pid=$($process.Id) principal_cap=$PrincipalCapUsd leverage=$Leverage markets=$($Markets -join ',') max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd bot_exe=$resolvedBotExePath"
}

Write-MonitorLog "monitor started poll_secs=$PollSecs base_url=$BaseUrl principal_cap=$PrincipalCapUsd leverage=$Leverage markets=$($Markets -join ',') max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd bot_exe=$BotExePath"

while ($true) {
    try {
        $status = Invoke-Json "/api/copy/live-soak/status"
        $soakProcess = Get-SoakProcess
        if ($status.ok -and $status.data.running -and $null -ne $soakProcess -and (Test-SoakHeartbeatStale)) {
            $diagnostic = "soak_heartbeat_stale; $(Get-SoakHeartbeatDiagnostic)"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            Send-MonitorNotification -Status "stopped" -Reason "soak_heartbeat_stale" -Detail $diagnostic
            try {
                Start-CopyLiveSoak
            } catch {
                Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                throw
            }
        } elseif ($status.ok -and $status.data.running -and $null -ne $soakProcess) {
            Write-MonitorLog "healthy pid=$($soakProcess.ProcessId) run_id=$($status.data.run_id) round=$($status.data.latest_round) message=$($status.data.message)"
        } elseif ($status.ok -and $status.data.running -and $null -eq $soakProcess) {
            $diagnostic = "frontend_running_but_soak_process_missing run_id=$($status.data.run_id) round=$($status.data.latest_round) message=$($status.data.message)"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            Send-MonitorNotification -Status "stopped" -Reason "soak_process_missing" -Detail $diagnostic
            try {
                Start-CopyLiveSoak
            } catch {
                Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                throw
            }
        } elseif ((Test-SoakProcessRunning) -and (Test-SoakHeartbeatStale)) {
            $diagnostic = "soak_heartbeat_stale_without_status; $(Get-SoakHeartbeatDiagnostic)"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            Send-MonitorNotification -Status "stopped" -Reason "soak_heartbeat_stale" -Detail $diagnostic
            try {
                Start-CopyLiveSoak
            } catch {
                Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                throw
            }
        } elseif (Test-SoakProcessRunning) {
            $diagnostic = if ($status.ok) { "status_not_running message=$($status.data.message)" } else { "status_error=$($status.error)" }
            $proc = Get-SoakProcess
            Write-MonitorLog "healthy_fallback pid=$($proc.ProcessId) $diagnostic"
        } else {
            $diagnostic = Get-LatestRunDiagnostic
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            Send-MonitorNotification -Status "stopped" -Reason "soak_not_running" -Detail $diagnostic
            try {
                Start-CopyLiveSoak
            } catch {
                Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                throw
            }
        }
    } catch {
        if (Test-SoakProcessRunning) {
            $proc = Get-SoakProcess
            if (Test-SoakHeartbeatStale) {
                $diagnostic = "status_exception=$($_.Exception.Message); $(Get-SoakHeartbeatDiagnostic)"
                Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
                Send-MonitorNotification -Status "stopped" -Reason "soak_heartbeat_stale" -Detail $diagnostic
                try {
                    Start-CopyLiveSoak
                } catch {
                    Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
                    throw
                }
            } else {
                Write-MonitorLog "healthy_fallback pid=$($proc.ProcessId) status_exception=$($_.Exception.Message)"
            }
        } else {
            $diagnostic = "status_exception=$($_.Exception.Message); no_soak_process=true"
            Write-MonitorLog "detected stopped soak; diagnostic: $diagnostic"
            Send-MonitorNotification -Status "stopped" -Reason "soak_not_running" -Detail $diagnostic
            try {
                Start-CopyLiveSoak
            } catch {
                Write-MonitorLog "monitor restart failed after status exception: $($_.Exception.Message)"
                Send-MonitorNotification -Status "failed" -Reason "restart_failed" -Detail $_.Exception.Message
            }
        }
    }
    Start-Sleep -Seconds $PollSecs
}
