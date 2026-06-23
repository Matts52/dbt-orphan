-- PASSES if incl_orphan_for_testing was dropped by cleanup_orphans with include_patterns=['incl_%']
-- Returns rows (FAILS) if the table still exists

select table_name
from information_schema.tables
where lower(table_schema) = lower('{{ target.schema }}')
    and lower(table_name) = 'incl_orphan_for_testing'
