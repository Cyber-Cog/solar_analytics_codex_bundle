import sys
sys.path.insert(0, 'backend')
from database import engine
from sqlalchemy import text

with engine.connect() as conn:
    r = conn.execute(text(
        "SELECT indexname, indexdef FROM pg_indexes WHERE tablename='raw_data_generic' ORDER BY indexname"
    )).fetchall()
    for row in r:
        print(row[0], ':', row[1])
