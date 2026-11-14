$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Write-Host "Prefer Docker Compose on Windows for the full stack:"
Write-Host "  docker compose --env-file .env up --build -d"
Write-Host "Or start services manually after setup.ps1."
