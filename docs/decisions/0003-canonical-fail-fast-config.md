# 0003 — One canonical, fail-fast configuration

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

The previous configuration surface accepted more than one schema shape,
versioned alternatives, an opt-in strict mode and environment names inherited
from earlier deployments. That made a partially applied configuration look
valid: an invalid file could be replaced by environment values, and a process
could start with a flow that no longer matched the documentation.

GoWhale is a server with one deployment unit. It does not need a compatibility
layer for configuration formats it no longer supports.

## Decision

- There is one YAML shape, documented in `docs/schemas/env-config.md` and
  exemplified by `config.example.yaml`. It has no `version` field.
- `--config <path>` takes precedence over `GOWHALE_CONFIG_FILE`. When neither is
  present, the process runs in environment-only mode.
- A file selected explicitly is authoritative. A read, parse or validation error
  is returned; it is never replaced by environment values.
- Unknown YAML keys are rejected (`KnownFields(true)`).
- Public environment variables use the `GOWHALE_` prefix. Within a loaded file,
  the environment overrides the values it is documented to override.
- Secrets and other credentials are environment-only in the published contract.
  `GOWHALE_QUESTDB_PG_PASSWORD` is required when the cold tier is enabled.
- `STRICT_CONFIG` does not exist. Validation is always strict.
- There is no v1/v2 parser, no legacy environment-name reader and no environment
  feature flag that selects between an old and a new flow.
- Defaults are local and safe. A declaration that cannot be read is an error,
  not a warning; `doctor` exits non-zero instead of reporting an unread verdict
  as healthy.

## Consequences

- Existing deployments must rename their environment variables and remove
  versioned or legacy fields before cutover. Old names are not read.
- Adding a configuration field requires updating the loader, the schema
  document, the example file and the tests in the same change.
- A malformed file cannot be masked by a correct environment: the operator sees
  the file error at startup.
- The process can be configured from the environment alone, but that mode has
  its own defaults and does not populate sections that only exist in YAML.
