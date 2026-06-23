-- PASSES if noincl_orphan_for_testing was NOT dropped by cleanup_orphans with include_patterns=['incl_%']
-- Returns rows (FAILS) if the table is missing (was incorrectly dropped)

select 'noincl_orphan_for_testing' as should_still_exist
where not exists (
    select 1
    from information_schema.tables
    where lower(table_schema) = lower('{{ target.schema }}')
        and lower(table_name) = 'noincl_orphan_for_testing'
)
