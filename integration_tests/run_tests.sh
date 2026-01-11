#!/bin/bash
set -e

echo "=== dbt-stale Integration Tests ==="
echo ""

echo "1. Installing dependencies..."
dbt deps

echo ""
echo "2. Running dbt models (creates kept_model, another_kept_model)..."
dbt run

echo ""
echo "3. Creating orphan tables (not in dbt graph)..."
dbt run-operation create_orphan_tables

echo ""
echo "4. Running cleanup_stale_by_graph (dry_run first)..."
dbt run-operation dbt_stale.cleanup_stale_by_graph --args '{dry_run: true}'

echo ""
echo "5. Running cleanup_stale_by_graph (actual cleanup)..."
dbt run-operation dbt_stale.cleanup_stale_by_graph --args '{dry_run: false}'

echo ""
echo "6. Running tests to verify results..."
dbt test

echo ""
echo "=== All tests passed! ==="
