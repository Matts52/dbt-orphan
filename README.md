# dbt-orphan

A dbt package that automatically cleans up orphaned database objects (tables and views) that are no longer defined in your dbt project.

## Tested Adapters

- PostgreSQL
- Snowflake
- DuckDB

## Installation

Add this package to your `packages.yml` or `dependencies.yml` in your dbt project:

```yaml
packages:
  - package: "Matts52/dbt_orphan"
    version: [">=0.1.0"]
```

Then run:

```bash
dbt deps
```

## Usage

### Recommended: Project-level Post-hook

Add `cleanup_orphans` as a post-hook in your `dbt_project.yml` to automatically clean up orphans after each run:

```yaml
on-run-end:
  - "{{ dbt_orphan.cleanup_orphans(schemas=['analytics', 'staging', 'marts'], dry_run=false) }}"
```

**Important:** You must explicitly list all schemas you want to clean. The macro will only remove orphans from the schemas you specify.

### Manual Execution

```bash
# Dry run - preview what would be dropped
dbt run-operation dbt_orphan.cleanup_orphans --args '{schemas: ["analytics", "staging"], dry_run: true}'

# Actual cleanup
dbt run-operation dbt_orphan.cleanup_orphans --args '{schemas: ["analytics", "staging"], dry_run: false}'
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | string | `target.database` | Database to scan |
| `schemas` | list | `[target.schema]` | List of schemas to scan for orphans |
| `dry_run` | boolean | `true` | When true, only logs what would be dropped |
| `exclude_patterns` | list | `[]` | SQL LIKE patterns for table names to exclude |

## Example with Exclude Patterns

```yaml
on-run-end:
  - "{{ dbt_orphan.cleanup_orphans(schemas=['analytics'], dry_run=false, exclude_patterns=['%_backup', 'tmp_%']) }}"
```

## Materializing Orphans as a Model

Use the `get_orphans` macro to create a model that lists all orphaned objects without dropping them. This is useful for reporting, auditing, or reviewing orphans before cleanup.

### Basic Usage

```sql
-- models/orphaned_objects.sql
{{ dbt_orphan.get_orphans() }}
```

### With Options

```sql
-- models/orphaned_objects.sql
{{ dbt_orphan.get_orphans(
    schemas=['analytics', 'staging', 'marts'],
    exclude_patterns=['%_backup', 'temp_%']
) }}
```

### Output Columns

| Column | Description |
|--------|-------------|
| `database_name` | The database containing the orphan |
| `schema_name` | The schema containing the orphan |
| `table_name` | The orphaned table or view name |
| `table_type` | Either 'BASE TABLE' or 'VIEW' |
| `full_name` | Fully qualified `schema.table_name` |

## How It Works

1. For each schema in the provided list, the macro collects all models, seeds, and snapshots defined in the dbt graph
2. Queries `information_schema.tables` for all existing tables and views in that schema
3. Compares the two sets and drops any objects not in the dbt graph
4. Logs all actions taken (or would be taken in dry_run mode)

## Safety

- **Always test with `dry_run: true` first** to preview what will be dropped
- Only schemas explicitly listed will be scanned
- Use `exclude_patterns` to protect tables that exist outside of dbt
