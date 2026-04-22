# Recompute fault_diagnostics for one plant (PostgreSQL, uses backend/.env)
# Usage: .\scripts\recompute_ds.ps1
#        .\scripts\recompute_ds.ps1 -Plant "NTPCNOKHRA"

param([string]$Plant = "NTPCNOKHRA")

$BackendRoot = Split-Path -Parent $PSScriptRoot
Set-Location $BackendRoot
python scripts/recompute_ds_faults.py --plant $Plant
