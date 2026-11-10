# Schemas

This directory documents the storage and configuration contracts that the code
in this repository implements. The code and its tests are the authority; when a
document and the code disagree, fix the document in the same change.

| File | Contract |
| --- | --- |
| [redis.md](redis.md) | Redis key families, TTLs, candle and derivatives member formats, last trade |
| [questdb.md](questdb.md) | QuestDB DDL, columns, dedup keys, write and retention semantics |
| [env-config.md](env-config.md) | YAML configuration shape and `GOWHALE_*` environment contract |

Other contracts are generated or owned elsewhere:

- The HTTP contract is generated from the router and served at `/openapi.json`;
  it is not duplicated here.
- The Redis and QuestDB adapters live under `internal/adapters/outbound/redis`
  and `internal/candle/engine`. Their contract tests are the executable form of
  these documents.
- The QuestDB init file is `scripts/questdb/questdb-init.sql`; the binary verifies
  the table contract at startup and adds additive columns that the file does not
  declare.
