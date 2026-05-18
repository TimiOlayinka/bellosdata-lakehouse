# ═══════════════════════════════════════════════════════════════
# BellosData — Deploy to Cloud Platform
# Push latest DAGs and configs to the Lightsail data platform
# ═══════════════════════════════════════════════════════════════
#
# Usage:
#   .\deploy-platform.ps1                         # Deploy latest code
#   .\deploy-platform.ps1 -SetupCredentials       # First-time: set IAM keys
#   .\deploy-platform.ps1 -Restart                # Restart all services
#
# Prerequisites:
#   - Lightsail instance running (terraform apply)
#   - SSH key at .lightsail-key.pem (downloaded from Lightsail console)
#
# ═══════════════════════════════════════════════════════════════

param(
    [switch]$SetupCredentials,
    [switch]$Restart,
    [string]$KeyFile = ""
)

$ErrorActionPreference = "Stop"

# ── Resolve Platform IP from Terraform ──
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path

# Try to get IP from terraform output, fall back to manual
try {
    Push-Location "$ROOT\terraform"
    $IP = (terraform output -raw data_platform_ip 2>$null)
    Pop-Location
} catch {
    Pop-Location
    $IP = ""
}

if (-not $IP) {
    Write-Host "Could not resolve data platform IP from Terraform." -ForegroundColor Yellow
    $IP = Read-Host "Enter Lightsail IP manually"
}

# ── Resolve SSH key ──
if (-not $KeyFile) {
    # Try common locations
    $keyLocations = @(
        "$ROOT\.lightsail-platform-key.pem",
        "$env:USERPROFILE\.ssh\lightsail-platform.pem",
        "$env:USERPROFILE\Downloads\LightsailDefaultKey-eu-west-2.pem"
    )
    foreach ($loc in $keyLocations) {
        if (Test-Path $loc) { $KeyFile = $loc; break }
    }
}

if (-not $KeyFile -or -not (Test-Path $KeyFile)) {
    Write-Host "SSH key not found. Download from Lightsail console:" -ForegroundColor Red
    Write-Host "  AWS Console → Lightsail → Account → SSH Keys → Download" -ForegroundColor Yellow
    Write-Host "  Save to: $ROOT\.lightsail-platform-key.pem" -ForegroundColor Yellow
    exit 1
}

$SSH = "ssh -i `"$KeyFile`" -o StrictHostKeyChecking=no ec2-user@$IP"
$SCP = "scp -i `"$KeyFile`" -o StrictHostKeyChecking=no"

Write-Host ""
Write-Host "══════════════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host "  BellosData Cloud Deploy — $IP" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════════════════" -ForegroundColor DarkCyan

if ($SetupCredentials) {
    Write-Host ""
    Write-Host "Setting up IAM credentials on cloud instance..." -ForegroundColor Yellow
    $AccessKey = Read-Host "AWS_ACCESS_KEY_ID"
    $SecretKey = Read-Host "AWS_SECRET_ACCESS_KEY" -AsSecureString
    $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecretKey)
    $SecretKeyPlain = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)

    $envContent = @"
AWS_ACCESS_KEY_ID=$AccessKey
AWS_SECRET_ACCESS_KEY=$SecretKeyPlain
AWS_DEFAULT_REGION=eu-west-2
"@
    $envContent | Out-File -FilePath "$ROOT\.env.cloud" -Encoding ascii -NoNewline
    Invoke-Expression "$SCP `"$ROOT\.env.cloud`" ec2-user@${IP}:/home/ec2-user/platform/.env"
    Remove-Item "$ROOT\.env.cloud" -ErrorAction SilentlyContinue
    Write-Host "  Credentials deployed!" -ForegroundColor Green
}

# ── Deploy latest DAGs ──
Write-Host ""
Write-Host "Syncing DAGs to cloud..." -ForegroundColor Yellow
Invoke-Expression "$SCP -r `"$ROOT\airflow\dags\*`" ec2-user@${IP}:/home/ec2-user/platform/airflow/dags/"
Write-Host "  DAGs synced" -ForegroundColor Green

# ── Deploy compose files ──
Write-Host "Syncing compose files..." -ForegroundColor Yellow
Invoke-Expression "$SCP `"$ROOT\airflow\docker-compose.cloud.yaml`" ec2-user@${IP}:/home/ec2-user/platform/airflow/docker-compose.cloud.yaml"
Invoke-Expression "$SCP `"$ROOT\unity-catalog\compose.cloud.yaml`" ec2-user@${IP}:/home/ec2-user/platform/unity-catalog/compose.cloud.yaml"
Write-Host "  Compose files synced" -ForegroundColor Green

# ── Deploy UC server properties ──
Write-Host "Syncing Unity Catalog config..." -ForegroundColor Yellow
Invoke-Expression "$SCP `"$ROOT\unity-catalog\etc\conf\server.cloud.properties`" ec2-user@${IP}:/home/ec2-user/platform/unity-catalog/etc/conf/server.properties"
Write-Host "  UC config synced" -ForegroundColor Green

if ($Restart) {
    Write-Host ""
    Write-Host "Restarting cloud services..." -ForegroundColor Yellow
    Invoke-Expression "$SSH 'cd /home/ec2-user/platform && docker compose -f unity-catalog/compose.cloud.yaml down && docker compose -f unity-catalog/compose.cloud.yaml up -d'"
    Invoke-Expression "$SSH 'cd /home/ec2-user/platform && docker compose -f airflow/docker-compose.cloud.yaml down && docker compose -f airflow/docker-compose.cloud.yaml up -d'"
    Write-Host "  Services restarted!" -ForegroundColor Green
}

Write-Host ""
Write-Host "══════════════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host "  Deploy Complete" -ForegroundColor Green
Write-Host "══════════════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host ""
Write-Host "  Airflow UI:        " -NoNewline; Write-Host "http://${IP}:8081" -ForegroundColor Cyan
Write-Host "  Unity Catalog UI:  " -NoNewline; Write-Host "http://${IP}:3000" -ForegroundColor Cyan
Write-Host "  Unity Catalog API: " -NoNewline; Write-Host "http://${IP}:8070" -ForegroundColor Cyan
Write-Host ""
