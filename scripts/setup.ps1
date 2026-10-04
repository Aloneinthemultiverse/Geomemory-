# One-shot setup for Windows PowerShell.
#   powershell -ExecutionPolicy Bypass -File scripts\setup.ps1          # full
#   powershell -ExecutionPolicy Bypass -File scripts\setup.ps1 -Core    # no Docker
param([switch]$Core)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
function Say($m) { Write-Host "`n== $m" -ForegroundColor Cyan }

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  throw "Python 3.10+ not found. Install from https://www.python.org/downloads/ (tick 'Add python.exe to PATH')."
}
python -c "import sys; assert sys.version_info >= (3, 10), 'need Python 3.10+'"

Say "Python virtual environment (.venv)"
if (-not (Test-Path .venv)) { python -m venv .venv }
. .\.venv\Scripts\Activate.ps1
python -m pip install -q --upgrade pip
if ($Core) { python -m pip install -q -e ".[api]" } else { python -m pip install -q -e ".[all]" }

Say "Real datasets (~200 MB, one time)"
python -m geomemory.datasets download

if (-not $Core) {
  if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker not found. Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
  }
  Say "Database (PostGIS + AGE) and Kafka in Docker"
  docker compose up -d --build db kafka
  for ($i = 0; $i -lt 60; $i++) {
    $db = docker inspect -f "{{.State.Health.Status}}" (docker compose ps -q db)
    $kf = docker inspect -f "{{.State.Health.Status}}" (docker compose ps -q kafka)
    if ($db -eq "healthy" -and $kf -eq "healthy") { break }
    Start-Sleep 3
  }
  docker compose ps
}

Say "Tests"
python -m unittest discover -s tests

Say "Done"
Write-Host "Activate in new terminals:  .\.venv\Scripts\Activate.ps1"
Write-Host "Demo UI:                    python -m geomemory.demo            -> http://127.0.0.1:8765"
Write-Host "Real Uber data:             python -m geomemory.demo --dataset uber"
Write-Host "FastAPI over PostGIS:       python -m geomemory.api --backend postgis"
