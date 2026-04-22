import os
import sys

import psycopg2
from psycopg2 import sql


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> int:
    host = get_required_env("RDS_HOST")
    port = int(os.getenv("RDS_PORT", "5432"))
    database = get_required_env("RDS_DATABASE")
    user = get_required_env("RDS_USER")
    password = get_required_env("RDS_PASSWORD")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
        sslmode=os.getenv("RDS_SSLMODE", "require"),
        connect_timeout=10,
    )

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user, version();")
            db_name, db_user, version = cur.fetchone()
            print(f"Connected to {db_name} as {db_user}")
            print(version)

            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
                """
            )
            tables = [row[0] for row in cur.fetchall()]
            print(f"Found {len(tables)} public tables")

            for table_name in tables:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(table_name))
                )
                count = cur.fetchone()[0]
                print(f"{table_name}: {count}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
