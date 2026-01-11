# Integration Tests

## Prerequisites

- PostgreSQL or DuckDB database
- Update `profiles.yml` with your connection details

## Running Tests

**Quick start:**
```bash
cd integration_tests
./run_tests.sh
```

**Manual steps:**
```bash
dbt deps
dbt run
dbt run-operation create_orphan_tables
dbt run-operation dbt_orphan.cleanup_orphans --args '{schemas: ["test_dbt_orphan"], dry_run: false}'
dbt test
```

## What's Tested

1. **Orphan cleanup** - Tables not in dbt graph are dropped
2. **Model preservation** - Models in the graph are kept

## Switching Adapters

Edit `dbt_project.yml` to change the profile:
```yaml
profile: "integration_tests_postgres"  # or integration_tests_duckdb
```
