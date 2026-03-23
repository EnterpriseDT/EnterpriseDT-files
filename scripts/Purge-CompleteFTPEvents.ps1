<#
.SYNOPSIS
    Purges old event records from CompleteFTP's events.db database.

.DESCRIPTION
    Interactively walks you through purging old event records from CompleteFTP's
    events.db database and reclaiming disk space.

    Requires sqlite3 CLI, either in PATH or placed next to this script.
    Download from https://sqlite.org/download.html (look for
    'sqlite-tools-win-x64').

    All parameters are optional - the script will prompt for any that aren't
    provided on the command line.

.PARAMETER DaysToKeep
    Number of days of event data to retain. Records older than this are deleted.

.PARAMETER EventsDbPath
    Path to the events.db file.

.PARAMETER NoServiceRestart
    Skip the service stop/restart step (use if you've already stopped the service).

.PARAMETER NonInteractive
    Run without prompts - uses defaults and skips confirmation. Suitable for
    scheduled tasks.

.EXAMPLE
    .\Purge-CompleteFTPEvents.ps1
    # Interactive mode - prompts for everything

.EXAMPLE
    .\Purge-CompleteFTPEvents.ps1 -DaysToKeep 90 -NonInteractive
    # Automated mode with 90-day retention
#>

param(
    [int]$DaysToKeep = 0,

    [string]$EventsDbPath = "",

    [switch]$NoServiceRestart,

    [switch]$NonInteractive
)

$ErrorActionPreference = "Stop"

$defaultEventsDbPath = "C:\ProgramData\Enterprise Distributed Technologies\Complete FTP\Logs\events.db"
$defaultDaysToKeep = 90

# --- Helper functions ---

function Write-Step([string]$message) {
    Write-Host ""
    Write-Host "--- $message ---" -ForegroundColor Cyan
}

function Write-Info([string]$message) {
    Write-Host "  $message"
}

function Write-OK([string]$message) {
    Write-Host "  $message" -ForegroundColor Green
}

function Write-Warn([string]$message) {
    Write-Host "  $message" -ForegroundColor Yellow
}

function Prompt-Value([string]$prompt, [string]$default) {
    $displayDefault = if ($default) { " [$default]" } else { "" }
    $value = Read-Host "  $prompt$displayDefault"
    if ([string]::IsNullOrWhiteSpace($value)) { return $default }
    return $value.Trim()
}

function Prompt-YesNo([string]$prompt, [bool]$default = $true) {
    $hint = if ($default) { "[Y/n]" } else { "[y/N]" }
    $value = Read-Host "  $prompt $hint"
    if ([string]::IsNullOrWhiteSpace($value)) { return $default }
    return $value.Trim().ToLower().StartsWith("y")
}

function Invoke-Sqlite3Query([string]$dbPath, [string]$sql) {
    $result = $sql | & $sqlite3Path $dbPath 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "sqlite3 error: $result"
    }
    return $result
}

function Invoke-Sqlite3Scalar([string]$dbPath, [string]$sql) {
    $result = Invoke-Sqlite3Query $dbPath $sql
    return [long]$result
}

function Stop-CompleteFTPService {
    if ($script:serviceStopped -or $NoServiceRestart) { return }

    $svc = $null
    if (Get-Command Get-Service -ErrorAction SilentlyContinue) {
        $svc = Get-Service "CompleteFTP" -ErrorAction SilentlyContinue
    }

    if (-not $svc -or $svc.Status -ne "Running") { return }

    # Check if we have admin rights
    $isAdmin = $false
    if ($env:OS -eq "Windows_NT") {
        $isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    }
    if (-not $isAdmin) {
        Write-Host ""
        Write-Warn "CompleteFTP service is currently running."
        Write-Warn "This script needs to be run as Administrator to stop/start the service."
        Write-Host ""
        Write-Info "Please either:"
        Write-Info "  1. Right-click PowerShell and select 'Run as Administrator', then re-run this script"
        Write-Info "  2. Stop the CompleteFTP service manually and re-run with -NoServiceRestart"
        exit 1
    }

    Write-Host ""
    Write-Info "CompleteFTP service is running. Stopping it to avoid database locking issues..."
    Stop-Service "CompleteFTP" -Force
    Start-Sleep -Seconds 3
    $script:serviceStopped = $true
    Write-OK "Service stopped."
}

$serviceStopped = $false

# --- Banner ---

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "  CompleteFTP Events Database Purge Tool" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  This tool removes old event records from CompleteFTP's events.db"
Write-Host "  database and reclaims disk space. Event data is used for the"
Write-Host "  Reports tab in the CompleteFTP Manager."
Write-Host ""

# --- Step 1: Find sqlite3 ---

Write-Step "Checking for sqlite3"

$sqlite3Cmd = Get-Command sqlite3 -ErrorAction SilentlyContinue
if (-not $sqlite3Cmd) {
    $localSqlite3 = Join-Path $PSScriptRoot "sqlite3.exe"
    if (Test-Path $localSqlite3) {
        $sqlite3Cmd = Get-Command $localSqlite3
    }
}
if (-not $sqlite3Cmd) {
    Write-Host ""
    Write-Host "  sqlite3 was not found." -ForegroundColor Red
    Write-Host ""
    Write-Host "  To fix this, download sqlite3.exe:" -ForegroundColor Yellow
    Write-Host "    1. Go to https://sqlite.org/download.html"
    Write-Host "    2. Under 'Precompiled Binaries for Windows', download 'sqlite-tools-win-x64'"
    Write-Host "    3. Extract sqlite3.exe and place it in the same folder as this script:"
    Write-Host "       $PSScriptRoot"
    Write-Host ""
    exit 1
}
$sqlite3Path = $sqlite3Cmd.Source
Write-OK "Found: $sqlite3Path"

# --- Step 2: Locate events.db ---

Write-Step "Locating events.db"

if ([string]::IsNullOrEmpty($EventsDbPath)) {
    if (Test-Path $defaultEventsDbPath) {
        Write-Info "Found events.db at the default location."
        $EventsDbPath = $defaultEventsDbPath
    } elseif (-not $NonInteractive) {
        Write-Warn "events.db not found at the default location:"
        Write-Info "$defaultEventsDbPath"
        Write-Host ""
        $EventsDbPath = Prompt-Value "Enter the full path to events.db" ""
    } else {
        Write-Host "  events.db not found at: $defaultEventsDbPath" -ForegroundColor Red
        exit 1
    }
}

if (-not (Test-Path $EventsDbPath)) {
    Write-Host "  File not found: $EventsDbPath" -ForegroundColor Red
    exit 1
}

$dbSizeMB = [math]::Round((Get-Item $EventsDbPath).Length / 1MB, 1)
Write-OK "Found: $EventsDbPath ($dbSizeMB MB)"

# --- Step 3: Analyze database ---

Write-Step "Analyzing database"

$dbPath = (Resolve-Path $EventsDbPath).Path
Invoke-Sqlite3Query $dbPath "PRAGMA journal_mode=WAL;" | Out-Null

$totalCount = Invoke-Sqlite3Scalar $dbPath "SELECT COUNT(*) FROM EventDatum;"
$oldestDate = Invoke-Sqlite3Query $dbPath "SELECT MIN(CreatedTime) FROM EventDatum;"
$newestDate = Invoke-Sqlite3Query $dbPath "SELECT MAX(CreatedTime) FROM EventDatum;"

Write-Info "Total event records: $($totalCount.ToString('N0'))"
Write-Info "Date range: $oldestDate to $newestDate"
Write-Info "Database size: $dbSizeMB MB"

if ($totalCount -eq 0) {
    Write-Host ""
    Write-OK "Database is empty - nothing to purge."
    exit 0
}

# --- Step 4: Backup ---

Write-Step "Backup"

$defaultBackupDir = "C:\ProgramData\Enterprise Distributed Technologies\Complete FTP\Backup"

if (-not $NonInteractive) {
    $backupIt = Prompt-YesNo "Would you like to back up the database before making changes?" $true
    if ($backupIt) {
        $timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
        $backupDir = Prompt-Value "Backup directory" $defaultBackupDir
        if (-not (Test-Path $backupDir)) {
            Write-Info "Creating directory: $backupDir"
            New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
        }
        $backupPath = Join-Path $backupDir "events_$timestamp.db"
        Write-Info "Copying to $backupPath ..."
        Copy-Item -Path $EventsDbPath -Destination $backupPath
        $backupSizeMB = [math]::Round((Get-Item $backupPath).Length / 1MB, 1)
        Write-OK "Backup saved ($backupSizeMB MB)."
        Write-Info "You can delete this file after verifying the purge was successful."
    } else {
        Write-Info "Skipping backup."
    }
} else {
    Write-Info "Skipping backup (non-interactive mode)."
}

# --- Step 5: Choose retention period ---

Write-Step "Choosing retention period"

# Show estimated file sizes for common retention periods
$retentionOptions = @(30, 60, 90, 120, 180, 365)
# Filter to options that would actually delete something, plus one that wouldn't
$now = Get-Date
Write-Info "Estimated database size for different retention periods:"
Write-Host ""
Write-Host "    Keep         Records kept   Est. size   Reduction" -ForegroundColor Cyan
Write-Host "    ----------   ------------   ---------   ---------" -ForegroundColor Cyan
$shownAll = $false
foreach ($days in $retentionOptions) {
    $cutoff = $now.AddDays(-$days).ToString("yyyy-MM-dd HH:mm:ss")
    $kept = Invoke-Sqlite3Scalar $dbPath "SELECT COUNT(*) FROM EventDatum WHERE CreatedTime >= '$cutoff';"
    $estMB = if ($totalCount -gt 0) { [math]::Round(($kept / $totalCount) * $dbSizeMB, 0) } else { 0 }
    $reduction = if ($dbSizeMB -gt 0) { [math]::Round((1 - $kept / $totalCount) * 100) } else { 0 }
    $label = "$days days"
    $reductionStr = if ($reduction -gt 0) { "-$reduction%" } else { "" }
    Write-Host "    $($label.PadRight(13))$($kept.ToString('N0').PadLeft(12))   ~$($estMB.ToString().PadLeft(4)) MB   $reductionStr"
    if ($kept -eq $totalCount) { $shownAll = $true; break }
}
if (-not $shownAll) {
    Write-Host "    $("All".PadRight(13))$($totalCount.ToString('N0').PadLeft(12))   $("$dbSizeMB".PadLeft(5)) MB"
}
Write-Host ""

if ($DaysToKeep -le 0) {
    if ($NonInteractive) {
        Write-Host "  -DaysToKeep is required in non-interactive mode." -ForegroundColor Red
        exit 1
    } else {
        Write-Info "Enter a number of days to see how many records would be deleted."
        Write-Info "Leave blank to exit without making changes."

        while ($true) {
            Write-Host ""
            $input = Prompt-Value "Days to keep (blank to exit)" ""

            if ([string]::IsNullOrWhiteSpace($input)) {
                Write-Host ""
                Write-Info "No changes were made."
                exit 0
            }

            $parsed = 0
            if (-not [int]::TryParse($input, [ref]$parsed) -or $parsed -le 0) {
                Write-Warn "Please enter a positive number."
                continue
            }

            $DaysToKeep = $parsed
            $cutoffDate = (Get-Date).AddDays(-$DaysToKeep).ToString("yyyy-MM-dd HH:mm:ss")
            $toDelete = Invoke-Sqlite3Scalar $dbPath "SELECT COUNT(*) FROM EventDatum WHERE CreatedTime < '$cutoffDate';"
            $toKeep = $totalCount - $toDelete
            $pct = if ($totalCount -gt 0) { [math]::Round(($toDelete / $totalCount) * 100, 1) } else { 0 }

            Write-Host ""
            Write-Info "  Keep last $DaysToKeep days (since $cutoffDate)"
            Write-Info "  Would delete: $($toDelete.ToString('N0')) records ($pct%)"
            Write-Info "  Would keep:   $($toKeep.ToString('N0')) records"

            if ($toDelete -eq 0) {
                Write-OK "No records older than $DaysToKeep days."
            } else {
                Write-Host ""
                $confirm = Prompt-YesNo "Proceed with $DaysToKeep days?" $true
                if ($confirm) { break }
                Write-Info "Enter a different number of days, or leave blank to exit."
            }
        }
    }
}

$cutoffDate = (Get-Date).AddDays(-$DaysToKeep).ToString("yyyy-MM-dd HH:mm:ss")
$toDelete = Invoke-Sqlite3Scalar $dbPath "SELECT COUNT(*) FROM EventDatum WHERE CreatedTime < '$cutoffDate';"
$toKeep = $totalCount - $toDelete

if ($toDelete -eq 0) {
    Write-Host ""
    Write-OK "No records older than $DaysToKeep days. Nothing to purge."
    exit 0
}

# --- Step 6: Purging ---

Write-Step "Purging"

if (-not $NonInteractive) {
    Write-Warn "This will permanently delete $($toDelete.ToString('N0')) event records older than $cutoffDate."
    Write-Host ""
    $confirm = Prompt-YesNo "Proceed?" $true
    if (-not $confirm) {
        Write-Host ""
        Write-Info "Cancelled. No changes were made."
        exit 0
    }
}

# Stop service now if it's running (deferred until we actually need to modify the database)
Stop-CompleteFTPService

$dbSizeBefore = (Get-Item $EventsDbPath).Length
foreach ($ext in @("-wal", "-shm")) {
    $sideCar = "$EventsDbPath$ext"
    if (Test-Path $sideCar) { $dbSizeBefore += (Get-Item $sideCar).Length }
}
$dbSizeBefore = $dbSizeBefore / 1MB

try {
    $batchSize = 50000
    $deleted = 0
    Write-Host ""
    Write-Info "Deleting records..."

    while ($deleted -lt $toDelete) {
        Invoke-Sqlite3Query $dbPath "DELETE FROM EventDatum WHERE rowid IN (SELECT rowid FROM EventDatum WHERE CreatedTime < '$cutoffDate' LIMIT $batchSize);"
        $remaining = Invoke-Sqlite3Scalar $dbPath "SELECT COUNT(*) FROM EventDatum WHERE CreatedTime < '$cutoffDate';"
        $deleted = $toDelete - $remaining
        $pct = [math]::Round(($deleted / $toDelete) * 100)
        Write-Info "  Progress: $($deleted.ToString('N0')) / $($toDelete.ToString('N0')) ($pct%)"
        if ($remaining -eq 0) { break }
    }

    Write-OK "All old records deleted."
    Write-Host ""
    Write-Info "Reclaiming disk space (VACUUM)... this may take a while for large databases."

    Invoke-Sqlite3Query $dbPath "VACUUM; PRAGMA wal_checkpoint(TRUNCATE);" | Out-Null

    # Measure total size including any remaining WAL/SHM files
    $dbSizeAfter = (Get-Item $EventsDbPath).Length
    foreach ($ext in @("-wal", "-shm")) {
        $sideCar = "$EventsDbPath$ext"
        if (Test-Path $sideCar) { $dbSizeAfter += (Get-Item $sideCar).Length }
    }
    $dbSizeAfter = $dbSizeAfter / 1MB
    $saved = $dbSizeBefore - $dbSizeAfter

    Write-Host ""
    Write-Host "=========================================" -ForegroundColor Green
    Write-Host "  Purge complete!" -ForegroundColor Green
    Write-Host "=========================================" -ForegroundColor Green
    Write-Host ""
    Write-Info "Records deleted:  $($toDelete.ToString('N0'))"
    Write-Info "Records remaining: $($toKeep.ToString('N0'))"
    Write-Info "Size before:      $([math]::Round($dbSizeBefore, 1)) MB"
    Write-Info "Size after:       $([math]::Round($dbSizeAfter, 1)) MB"
    Write-Info "Space saved:      $([math]::Round($saved, 1)) MB"

} finally {
    if ($serviceStopped) {
        Write-Host ""
        Write-Info "Restarting CompleteFTP service..."
        Start-Service "CompleteFTP"
        Write-OK "Service started."
    }
}
