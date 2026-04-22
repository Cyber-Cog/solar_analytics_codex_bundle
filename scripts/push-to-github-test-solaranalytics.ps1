<#
  Push the whole project to GitHub under one folder:

    solaranalytics codex bundle/
      backend/
      frontend/
      database/
      scripts/
      .gitignore
      (everything else at repo root, except excluded junk)

  Remote:
    https://github.com/Ayushmishra449/Test_Solaranalytics080426.git
  Branch: main

  Usage (repo root — paths with spaces are OK):
    powershell -ExecutionPolicy Bypass -File ".\scripts\push-to-github-test-solaranalytics.ps1"

  Or: scripts\run-push-to-github.cmd

  Requires: git on PATH, GitHub auth (Credential Manager / PAT).

  If the remote already has commits, merge or use an empty repo first.
#>

[CmdletBinding()]
param(
    [string] $SourceRoot = "",
    [string] $RemoteUrl = "https://github.com/Ayushmishra449/Test_Solaranalytics080426.git",
    [string] $Branch = "main",
    [string] $BundleFolderName = "solaranalytics codex bundle",
    [string] $CommitMessage = "Import solar analytics codex bundle"
)

$ErrorActionPreference = "Stop"

if (-not $SourceRoot) {
    $SourceRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
}
$SourceRoot = (Resolve-Path -LiteralPath $SourceRoot).Path

$staging = Join-Path $env:TEMP ("solaranalytics_github_push_" + [Guid]::NewGuid().ToString("N"))
$bundlePath = Join-Path $staging $BundleFolderName

try {
    New-Item -ItemType Directory -Path $bundlePath -Force | Out-Null

    # IMPORTANT: Do not use Start-Process for robocopy — paths with spaces break into multiple
    # arguments. Call robocopy directly so each path is one argument.
    # Exit codes 0–7 = success for robocopy; 8+ = failure.
    $roboArgs = @(
        $SourceRoot,
        $bundlePath,
        "/E",
        "/NFL", "/NDL", "/NJH", "/NJS", "/NC", "/NS",
        "/XD", ".git", "__pycache__", "node_modules", "venv", ".venv", "env", ".mypy_cache", ".pytest_cache", "dist", "build", ".vscode", ".idea", ".cursor",
        "/XF", ".env", "*.pyc", "*.pyo"
    )
    & robocopy.exe @roboArgs
    $roboCode = $LASTEXITCODE
    if ($roboCode -ge 8) {
        throw "robocopy failed (exit $roboCode): $SourceRoot -> $bundlePath"
    }

    Push-Location $staging
    try {
        git init
        git branch -M $Branch
        git remote add origin $RemoteUrl
        git add -A
        $status = git status --porcelain
        if (-not $status) {
            throw "Nothing to commit (staging empty or all ignored)."
        }
        git commit -m $CommitMessage
        Write-Host ""
        Write-Host "Pushing to $RemoteUrl ($Branch) ..." -ForegroundColor Cyan
        git push -u origin $Branch
    }
    finally {
        Pop-Location
    }

    Write-Host ""
    Write-Host "Done. Remote root contains folder: $BundleFolderName" -ForegroundColor Green
}
finally {
    if (Test-Path -LiteralPath $staging) {
        Remove-Item -LiteralPath $staging -Recurse -Force -ErrorAction SilentlyContinue
    }
}
