$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
& "$Root\.venv\Scripts\python.exe" -m pytest "$Root\tests" -v
Push-Location "$Root\backend"; .\mvnw.cmd test; Pop-Location
Push-Location "$Root\frontend"; npm test; Pop-Location
