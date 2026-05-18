# ═══════════════════════════════════════════════════════════════
# BellosData Platform — Full Stack Startup Script
# Starts Unity Catalog + Airflow in one command
# ═══════════════════════════════════════════════════════════════
#
# Usage:
#   .\start-platform.ps1          # Start everything
#   .\start-platform.ps1 -Stop    # Stop everything
#
# ═══════════════════════════════════════════════════════════════

param(
    [switch]$Stop
)

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "══════════════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host "  BellosData Platform — Sovereign Data Engine" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host ""

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path

if ($Stop) {
    Write-Host "Stopping all services..." -ForegroundColor Yellow

    Write-Host "  Stopping Unity Catalog..." -ForegroundColor DarkGray
    Push-Location "$ROOT\unity-catalog"
    docker compose down
    Pop-Location

    Write-Host "  Stopping Airflow..." -ForegroundColor DarkGray
    Push-Location "$ROOT\airflow"
    docker compose down
    Pop-Location

    Write-Host ""
    Write-Host "All services stopped." -ForegroundColor Green
    exit 0
}

# ── Start Unity Catalog ──
Write-Host "Starting Unity Catalog..." -ForegroundColor Yellow
Push-Location "$ROOT\unity-catalog"
docker compose up -d
Pop-Location

# ── Start Airflow ──
Write-Host ""
Write-Host "Starting Airflow..." -ForegroundColor Yellow
Push-Location "$ROOT\airflow"
docker compose up -d
Pop-Location

# ── Health Check ──
Write-Host ""
Write-Host "Waiting for services to start..." -ForegroundColor DarkGray
Start-Sleep -Seconds 10

Write-Host ""
Write-Host "══════════════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host "  BellosData Platform is RUNNING" -ForegroundColor Green
Write-Host "══════════════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host ""
Write-Host "  Unity Catalog UI:    " -NoNewline; Write-Host "http://localhost:3000" -ForegroundColor Cyan
Write-Host "  Unity Catalog API:   " -NoNewline; Write-Host "http://localhost:8070" -ForegroundColor Cyan
Write-Host "  Airflow UI:          " -NoNewline; Write-Host "http://localhost:8081" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Register tables:     " -NoNewline; Write-Host "python unity-catalog/scripts/register_tables.py" -ForegroundColor DarkYellow
Write-Host "  Stop all services:   " -NoNewline; Write-Host ".\start-platform.ps1 -Stop" -ForegroundColor DarkYellow
Write-Host ""
