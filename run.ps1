# One command to run GeoMemory on Windows:
#   powershell -ExecutionPolicy Bypass -File run.ps1          demo world
#   powershell -ExecutionPolicy Bypass -File run.ps1 uber     1.8M real Uber pickups
param([string]$Mode = "demo")
$ErrorActionPreference = "Stop"
$env:PIP_DISABLE_PIP_VERSION_CHECK = "1"
Set-Location $PSScriptRoot
if (-not (Get-Command python -ErrorAction SilentlyContinue)) { throw "Please install Python 3.10+ from https://www.python.org/downloads/ (tick 'Add python.exe to PATH')." }
if (-not (Test-Path .venv\Scripts\python.exe)) { Write-Host "First run: setting up (about a minute)..."; python -m venv .venv }
$VPY = ".venv\Scripts\python.exe"
& $VPY -c "import geomemory, tzdata" 2>$null
if ($LASTEXITCODE -ne 0) { & $VPY -m pip install -q -e . }
$argsList = @("-m", "geomemory.demo", "--port", "8765")
if ($Mode -eq "uber") { & $VPY -m geomemory.datasets download; $argsList += @("--dataset", "uber") }
Start-Job { Start-Sleep 3; Start-Process "http://127.0.0.1:8765" } | Out-Null
Write-Host "GeoMemory is starting at http://127.0.0.1:8765  (Ctrl+C to stop)"
& $VPY @argsList
