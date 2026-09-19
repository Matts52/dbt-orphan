# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

dbt-stale is a dbt package that identifies and removes orphaned database objects (tables/views) that are no longer defined in the dbt project graph. This helps clean up stale artifacts after models are renamed or deleted.

## Key Commands

### Running Integration Tests
```bash
cd integration_tests
./run_tests.sh
```

### Manual Test Steps
```bash
cd integration_tests
dbt deps
dbt run
dbt run-operation create_orphan_tables
dbt run-operation dbt_stale.cleanup_orphans --args '{dry_run: false}'
dbt test
```

### Using the Package
```bash
# Dry run (preview what would be dropped)
dbt run-operation dbt_stale.cleanup_orphans --args '{dry_run: true}'

# Actual cleanup
dbt run-operation dbt_stale.cleanup_orphans --args '{dry_run: false}'

# With exclude patterns
dbt run-operation dbt_stale.cleanup_orphans --args '{dry_run: false, exclude_patterns: ["%_backup", "temp_%"]}'

# Multiple schemas
dbt run-operation dbt_stale.cleanup_orphans --args '{schemas: ["public", "analytics", "staging"], dry_run: false}'
```

## Architecture

### Core Macro (`macros/cleanup_orphans.sql`)
- `cleanup_orphans`: Main entry point that builds a set of dbt-managed objects from `graph.nodes` and dispatches to adapter-specific implementations
- `default__cleanup_orphans`: Default implementation using `information_schema.tables`
- `snowflake__cleanup_orphans`: Snowflake-specific implementation with proper column casing (uppercase `TABLE_NAME`, `TABLE_TYPE`)
- `redshift__cleanup_orphans`: Redshift-specific implementation that matches objects against `node.alias` instead of `node.name`, since Redshift tables/views are physically named after the alias

### How It Works
1. Iterates through provided schemas (or defaults to target.schema)
2. For each schema, collects all models, seeds, and snapshots from `graph.nodes`
3. Queries `information_schema.tables` for all existing tables/views in that schema
4. Compares the two sets and drops objects not in the dbt graph
5. Supports `dry_run` mode and `exclude_patterns` for safe operation

### Integration Tests (`integration_tests/`)
- Uses local package reference (`packages.yml: local: ../`)
- `create_orphan_tables` macro creates test orphans that should be cleaned up
- Tests verify orphans are dropped AND kept models survive
- Supports PostgreSQL and DuckDB adapters for local testing (switch via `profile` in `dbt_project.yml`); Snowflake and Redshift are supported via adapter-specific macros but have no integration test coverage

## dbt Version Requirements
- Requires dbt >= 1.1.0 and < 3.0.0
