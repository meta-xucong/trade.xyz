param(
    [string]$BaseUrl = "http://127.0.0.1:18844",
    [int]$PollSecs = 60,
    [int]$WindowSecs = 600,
    [double]$PrincipalCapUsd = 35.0,
    [double]$MaxTotalNotionalUsd = 700.0,
    [double]$MaxTotalFeesUsd = 1.0,
    [string]$AccountId = "addr_a",
    [string[]]$Leaders = @(
        "0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
        "0x6ac0b46b32dc429dbd129a503292f88649d2b8a0"
    ),
    [double]$CopyRatio = 0.2,
    [string]$LogPath = ".codex-longrun\copy-health-monitor.log",
    [string]$VaultPasswordEnv = "TRADE_XYZ_VAULT_PASSWORD"
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
            $report = Get-Content -LiteralPath $data.latest_report_path -Raw -Encoding utf8 | ConvertFrom-Json
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
    return $Process.Name -match "^(powershell|pwsh)\.exe$" `
        -and $commandLine -like "*run-persistent-live-soak.ps1*" `
        -and $commandLine -match "(?i)(^|\\s)-File\\s+" `
        -and $commandLine -notlike "*Get-CimInstance*"
}

function Test-SoakProcessRunning {
    return $null -ne (Get-SoakProcess)
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
        copy_ratio = $CopyRatio
        principal_cap_usd = $PrincipalCapUsd
        account_id = $AccountId
    }
    if (-not $settings.ok) {
        throw "copy settings update failed: $($settings.error)"
    }
    $notionalCap = $PrincipalCapUsd * 5.0
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
        $_.Name -match "^(powershell|pwsh)\.exe$" -and
        $_.CommandLine -like "*run-persistent-live-soak.ps1*" -and
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
    $body = @{
        window_secs = $WindowSecs
        max_rounds = 0
        max_total_notional_usd = $MaxTotalNotionalUsd
        max_total_fees_usd = $MaxTotalFeesUsd
        hold_positions_after_submit = $true
        confirm_mainnet_live = $true
    }
    $start = Invoke-Json "/api/copy/live-soak/start" "POST" $body
    if ($start.ok) {
        $soakPid = Set-LatestSoakPid $startedAt
        Write-MonitorLog "restart requested run_id=$($start.data.status.run_id) pid=$soakPid principal_cap=$PrincipalCapUsd max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd"
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
        "-HoldPositionsAfterSubmit"
    )
    $process = Start-Process -FilePath powershell -ArgumentList $args -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru
    Set-Content -LiteralPath ".codex-longrun\persistent-live-soak-detached-current.pid" -Value ([string]$process.Id) -Encoding ascii
    Write-MonitorLog "direct soak restart requested pid=$($process.Id) principal_cap=$PrincipalCapUsd max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd"
}

Write-MonitorLog "monitor started poll_secs=$PollSecs base_url=$BaseUrl principal_cap=$PrincipalCapUsd max_total_notional=$MaxTotalNotionalUsd max_total_fees=$MaxTotalFeesUsd"

while ($true) {
    try {
        $status = Invoke-Json "/api/copy/live-soak/status"
        if ($status.ok -and $status.data.running) {
            Write-MonitorLog "healthy run_id=$($status.data.run_id) round=$($status.data.latest_round) message=$($status.data.message)"
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
            Write-MonitorLog "healthy_fallback pid=$($proc.ProcessId) status_exception=$($_.Exception.Message)"
        } else {
            Write-MonitorLog "monitor loop error: $($_.Exception.Message)"
            Send-MonitorNotification -Status "failed" -Reason "monitor_loop_error" -Detail $_.Exception.Message
        }
    }
    Start-Sleep -Seconds $PollSecs
}
