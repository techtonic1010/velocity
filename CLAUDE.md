# CLAUDE.md — Stable Project Context

This file is read automatically at the start of every session. Keep it lean — full design rationale, data flow, and milestone details live in `PROJECT_SPEC.md`; read that separately when starting a milestone.

## What this project is
A two-stage (retrieval → ranking) news recommendation engine, built on the MIND-small dataset, demonstrating vector-hash sharding, a dual-sharded neighbor index, Kafka-based propagation, Redis-backed serving state, and Bloom-filter-based seen-item filtering.

## Tech stack
- Java 21 / Spring Boot — Entity Upload Service, Entity Interaction Service, Recommendation Service
- Python 3.11 / FastAPI — Embedding Creator, vector hashing / LSH, neighbor computation
- PostgreSQL — Entity DB, Entity History DB, Neighbor Index (both write and read copies)
- Redis — last-N entity cache + Bloom filter, sharded via `hash(userId) % NUM_SHARDS`
- Kafka (KRaft mode, no Zookeeper) — interaction events (key = userId), neighbor index resync (key = entityId). Inside Docker, services connect via `kafka:29092` (INTERNAL listener), not `localhost:9092`.
- Docker Compose — local orchestration for all of the above

## Folder conventions
- One top-level directory per service (see `PROJECT_SPEC.md` §9 for the full tree).
- Java services: standard Spring Boot Maven layout (`src/main/java`, `src/test/java`).
- Python services: FastAPI app in `app/`, tests in `tests/`, `requirements.txt` per service — no shared monorepo Python venv.

## Coding style
- Java: standard Spring Boot conventions, constructor injection only (no field `@Autowired`).
- Python: type hints required on all function signatures, `black` formatting.
- No inline secrets or API keys anywhere — use `.env` files, already gitignored.

## Build / test commands
- Java services: `mvn clean install` then `mvn test` inside each service directory.
- Python services: `pytest` inside each service directory.
- Full local stack: `docker-compose up` from repo root.

## Don't touch
- `scripts/mind-loader/` is a one-off ETL script, not a service — don't refactor it into the service architecture.
- Shard counts (`NUM_SHARDS`) are config-driven constants — don't hardcode shard math inline anywhere.

## Working agreement
- Always enter Plan Mode before editing code for any non-trivial change.
- Always run and show real test/build output before declaring a task done — never accept "should work" as verification.
- Log any simplification or trade-off made during implementation in `PROJECT_SPEC.md` §10 (Defensibility Tracker).
