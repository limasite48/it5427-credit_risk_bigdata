$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Write-Host "==> Setup (Windows)"
if (-not (Test-Path "$Root\.env")) {
  Copy-Item "$Root\.env.example" "$Root\.env"
  Write-Host "Created .env"
}
python -m venv "$Root\.venv"
& "$Root\.venv\Scripts\python.exe" -m pip install -r "$Root\requirements.txt"
Push-Location "$Root\frontend"; npm install; Pop-Location
Push-Location "$Root\backend"; .\mvnw.cmd -DskipTests dependency:resolve; Pop-Location
Write-Host "Setup complete. Run .\scripts\start.ps1"
