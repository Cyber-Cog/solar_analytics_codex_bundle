"""One-off: print DATABASE_URL host/user only (no password). Delete after use."""
from pathlib import Path
from urllib.parse import urlparse

p = Path(__file__).resolve().parent / ".env"
if not p.is_file():
    print("NO_ENV")
    raise SystemExit(1)
raw = None
for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
    s = line.strip()
    if s.startswith("DATABASE_URL=") and not s.startswith("#"):
        raw = s.split("=", 1)[1].strip().strip('"').strip("'")
        break
if not raw:
    print("DATABASE_URL missing")
    raise SystemExit(1)
u = urlparse(raw)
print("scheme:", u.scheme)
print("hostname:", u.hostname)
print("port:", u.port)
print("username length:", len(u.username or ""))
print("username preview:", (u.username or "")[:12] + ("…" if u.username and len(u.username) > 12 else ""))
print("path (db name):", (u.path or "")[:40])
