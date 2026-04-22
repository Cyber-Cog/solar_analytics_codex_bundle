"""
database/init_db.py — DEPRECATED

The application uses PostgreSQL only. Tables are created on backend startup via
SQLAlchemy (see backend/main.py: Base.metadata.create_all).

Do not use this script. Configure backend/.env with DATABASE_URL and start the
FastAPI server once to create schema.

Legacy SQLite + schema.sql flow has been removed.
"""
import sys

def main():
    print(
        "init_db.py is deprecated. Use PostgreSQL:\n"
        "  1. Set DATABASE_URL in backend/.env (e.g. postgresql://solar:solar@localhost:5432/solar)\n"
        "  2. Start the backend (uvicorn) — tables are created automatically.\n"
    )
    sys.exit(1)

if __name__ == "__main__":
    main()
