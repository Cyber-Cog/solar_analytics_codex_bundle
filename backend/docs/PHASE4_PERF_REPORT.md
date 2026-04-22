# Phase 4 — operational performance checklist (no auto-infra changes)

## Purpose

Use this document to decide whether slowness is dominated by **code**, **Vercel cold starts**, or **database / network**, before changing hosting or schema.

## 1. Vercel / serverless cold start

**Symptoms:** First request after idle takes several seconds; subsequent requests are fast; `/health` is slow only on first hit.

**Checks:**

- Compare response time of `GET /health` cold vs warm (repeat after 10–15 minutes idle).
- Enable Vercel function logs and note init duration vs handler duration.

**Mitigations (ops, not code):** provisioned concurrency, keep-warm ping (respect provider ToS), or move API to a long-lived process (Docker/ECS/EC2).

## 2. AWS RDS / connection path

**Symptoms:** Every API call is slow uniformly; DB CPU high in RDS console.

**Checks:**

- Ensure app and RDS are in the **same region**.
- On Vercel + RDS, expect extra latency vs same-VPC EC2; `NullPool` implies **new TCP/TLS per request** — watch connection count and RDS `max_connections`.

**Mitigations:** RDS Proxy, larger instance, read replica + `DATABASE_URL_READ` (already supported), Redis for `REDIS_URL` (cache hit rate).

## 3. PostgreSQL query / indexes

**Symptoms:** Specific endpoints slow (e.g. `/api/faults/unified-feed`, `/api/dashboard/bundle`).

**Checks:**

- Enable `SQL_ECHO=1` briefly in staging **only**.
- Run `EXPLAIN (ANALYZE, BUFFERS)` on representative SQL for slow routes.
- Confirm `db_perf` indexes exist (`idx_rdg_*`, `idx_fd_*`, etc.) and `ANALYZE` has run on large tables.

**Mitigations:** add/adjust indexes (often via `db_perf` / manual migration), narrow date ranges in UI, keep using bundled endpoints.

## 4. Frontend bundle

**Checks:**

- Chrome Performance + Network: time from navigation to **boot-complete** (`localStorage.solar_perf_log = 1` logs phases).
- Confirm heavy pages load only after route chunk (`route-chunk:...` log).

## Summary

Treat **cold start** and **cross-region DB** as platform limits; treat **missing indexes** and **too many round-trips** as code/schema work. This repo already uses dashboard bundle cache, gzip, and optional Redis — verify those in production.

