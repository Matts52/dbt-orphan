-- This test PASSES if orphan tables were successfully dropped
-- Returns rows if any orphan tables still exist (which means test FAILS)

select table_name
from information_schema.tables
where lower(table_schema) = lower('{{ target.schema }}')
    and lower(table_name) in (
        'orphan_table_for_testing',
        'orphan_view_for_testing',
        'old_model_that_was_renamed'
    )
