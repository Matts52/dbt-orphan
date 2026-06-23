#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INT_TESTS_DIR="$REPO_ROOT/integration_tests"

echo "=== dbt-orphan Integration Tests (DuckDB) ==="
echo ""

mkdir -p "$REPO_ROOT/duckdb"

cd "$INT_TESTS_DIR"

echo "1. Installing dependencies..."
dbt deps --profile integration_tests_duckdb

echo ""
echo "2. Running dbt models..."
dbt run --profile integration_tests_duckdb

echo ""
echo "3. Creating orphan tables..."
dbt run-operation create_orphan_tables --profile integration_tests_duckdb

echo ""
echo "4. Running cleanup_orphans (dry run)..."
dbt run-operation dbt_orphan.cleanup_orphans \
  --args '{schemas: ["test_dbt_orphan"], dry_run: true}' \
  --profile integration_tests_duckdb

echo ""
echo "5. Running cleanup_orphans (actual cleanup)..."
dbt run-operation dbt_orphan.cleanup_orphans \
  --args '{schemas: ["test_dbt_orphan"], dry_run: false}' \
  --profile integration_tests_duckdb

echo ""
echo "6. Creating include_pattern test tables..."
dbt run-operation create_include_pattern_test_tables --profile integration_tests_duckdb

echo ""
echo "7. Running cleanup_orphans with include_patterns (should only drop incl_orphan_for_testing)..."
dbt run-operation dbt_orphan.cleanup_orphans \
  --args '{schemas: ["test_dbt_orphan"], dry_run: false, include_patterns: ["incl_%"]}' \
  --profile integration_tests_duckdb

echo ""
echo "8. Running tests..."
dbt test --profile integration_tests_duckdb

echo ""
echo "9. Final cleanup of remaining test artifacts..."
dbt run-operation dbt_orphan.cleanup_orphans \
  --args '{schemas: ["test_dbt_orphan"], dry_run: false}' \
  --profile integration_tests_duckdb

echo ""
echo "=== All tests passed! ==="
