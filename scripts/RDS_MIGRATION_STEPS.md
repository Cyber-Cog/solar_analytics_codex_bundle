## Inputs You Still Need

- Local database name
- Correct local PostgreSQL password for user `postgres`
- AWS RDS password for user `postgres`

## AWS RDS Details

- Host: `database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com`
- Port: `5432`
- Database: `postgres`
- User: `postgres`

## Security Group Check

Ensure the RDS security group allows inbound TCP `5432` from the IP that will connect:

1. AWS Console -> RDS -> your instance -> `Connectivity & security`
2. Open the attached security group
3. Inbound rules -> Add rule:
4. Type: `PostgreSQL`
5. Port: `5432`
6. Source:
   - your public IP `/32` for a secure setup
   - or Replit outbound IP/CIDR if you know it

Do not leave `0.0.0.0/0` open unless this is only temporary.

## Export Local PostgreSQL

```powershell
$env:PGPASSWORD = "YOUR_LOCAL_PASSWORD"
& "C:\Program Files\PostgreSQL\18\bin\pg_dump.exe" `
  --format=custom `
  --verbose `
  --no-owner `
  --no-privileges `
  --host localhost `
  --port 5432 `
  --username postgres `
  --dbname YOUR_LOCAL_DB `
  --file .\local_db.dump
```

## Test AWS RDS Connectivity

```powershell
$env:PGPASSWORD = "YOUR_RDS_PASSWORD"
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" `
  --host database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com `
  --port 5432 `
  --username postgres `
  --dbname postgres `
  --command "SELECT current_database(), current_user, version();"
```

## Restore to AWS RDS

```powershell
$env:PGPASSWORD = "YOUR_RDS_PASSWORD"
& "C:\Program Files\PostgreSQL\18\bin\pg_restore.exe" `
  --verbose `
  --clean `
  --if-exists `
  --no-owner `
  --no-privileges `
  --host database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com `
  --port 5432 `
  --username postgres `
  --dbname postgres `
  .\local_db.dump
```

## Verify Transfer

### Compare table list

```powershell
$env:PGPASSWORD = "YOUR_RDS_PASSWORD"
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" `
  --host database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com `
  --port 5432 `
  --username postgres `
  --dbname postgres `
  --command "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;"
```

### Compare row counts

Run this on both local and RDS:

```sql
SELECT relname AS table_name, n_live_tup AS approx_rows
FROM pg_stat_user_tables
ORDER BY relname;
```

For exact counts on important tables:

```sql
SELECT COUNT(*) FROM your_table_name;
```

## Python Connection Script

Install:

```powershell
python -m pip install psycopg2-binary
```

Run:

```powershell
$env:RDS_HOST = "database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com"
$env:RDS_PORT = "5432"
$env:RDS_DATABASE = "postgres"
$env:RDS_USER = "postgres"
$env:RDS_PASSWORD = "YOUR_RDS_PASSWORD"
python .\scripts\connect_rds_psycopg2.py
```

## Replit Connection String

```text
postgresql://postgres:YOUR_RDS_PASSWORD@database-1.c92kau6ymg43.eu-north-1.rds.amazonaws.com:5432/postgres?sslmode=require
```

## One-Step Script

After replacing real values, you can run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\migrate_pg_to_rds.ps1 `
  -LocalDatabase "YOUR_LOCAL_DB" `
  -LocalPassword "YOUR_LOCAL_PASSWORD" `
  -RdsPassword "YOUR_RDS_PASSWORD"
```

## Common Errors

- `password authentication failed`
  Use the correct password and confirm the DB user.

- `no pg_hba.conf entry` or timeout
  The RDS security group is blocking access, or the instance is not publicly reachable.

- `database does not exist`
  Check the database name on local or RDS.

- `must be owner of relation`
  Use `--no-owner --no-privileges` during restore, or restore as the object owner.

- `unsupported version`
  Use PostgreSQL 18 client tools, which are installed at `C:\Program Files\PostgreSQL\18\bin`.
