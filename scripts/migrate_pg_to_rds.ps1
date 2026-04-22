param(
    [string]$LocalHost = "localhost",
    [int]$LocalPort = 5432,
    [string]$LocalDatabase,
    [string]$LocalUser = "postgres",
    [string]$LocalPassword,
    [string]$RdsHost = "database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com",
    [int]$RdsPort = 5432,
    [string]$RdsDatabase = "postgres",
    [string]$RdsUser = "postgres",
    [string]$RdsPassword,
    [string]$PgBin = "C:\Program Files\PostgreSQL\18\bin",
    [string]$DumpFile = ".\local_db.dump"
)

$ErrorActionPreference = "Stop"

if (-not $LocalDatabase) {
    throw "LocalDatabase is required."
}

if (-not $LocalPassword) {
    throw "LocalPassword is required."
}

if (-not $RdsPassword) {
    throw "RdsPassword is required."
}

$pgDump = Join-Path $PgBin "pg_dump.exe"
$pgRestore = Join-Path $PgBin "pg_restore.exe"
$psql = Join-Path $PgBin "psql.exe"

foreach ($tool in @($pgDump, $pgRestore, $psql)) {
    if (-not (Test-Path -LiteralPath $tool)) {
        throw "Missing PostgreSQL tool: $tool"
    }
}

Write-Host "Step 1/4: Exporting local database..." -ForegroundColor Cyan
$env:PGPASSWORD = $LocalPassword
& $pgDump `
    --format=custom `
    --verbose `
    --no-owner `
    --no-privileges `
    --host $LocalHost `
    --port $LocalPort `
    --username $LocalUser `
    --dbname $LocalDatabase `
    --file $DumpFile

if ($LASTEXITCODE -ne 0) {
    throw "pg_dump failed."
}

Write-Host "Step 2/4: Testing AWS RDS connectivity..." -ForegroundColor Cyan
$env:PGPASSWORD = $RdsPassword
& $psql `
    --host $RdsHost `
    --port $RdsPort `
    --username $RdsUser `
    --dbname $RdsDatabase `
    --command "SELECT current_database(), current_user, version();"

if ($LASTEXITCODE -ne 0) {
    throw "RDS connectivity test failed. Check endpoint, password, and security group."
}

Write-Host "Step 3/4: Restoring dump to AWS RDS..." -ForegroundColor Cyan
& $pgRestore `
    --verbose `
    --clean `
    --if-exists `
    --no-owner `
    --no-privileges `
    --host $RdsHost `
    --port $RdsPort `
    --username $RdsUser `
    --dbname $RdsDatabase `
    $DumpFile

if ($LASTEXITCODE -ne 0) {
    throw "pg_restore failed."
}

Write-Host "Step 4/4: Listing table row counts on AWS RDS..." -ForegroundColor Cyan
$verifySql = @"
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
"@

& $psql `
    --host $RdsHost `
    --port $RdsPort `
    --username $RdsUser `
    --dbname $RdsDatabase `
    --command $verifySql

if ($LASTEXITCODE -ne 0) {
    throw "Verification query failed."
}

Write-Host "Migration completed successfully." -ForegroundColor Green
