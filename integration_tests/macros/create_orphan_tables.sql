{% macro create_orphan_tables() %}
    {#
        Creates tables and views that are NOT in the dbt graph.
        These should be cleaned up by cleanup_orphans.
    #}

    {{ log('Creating orphan tables for testing...', info=true) }}

    {% set orphan_table_sql %}
        create table if not exists test_dbt_orphan.orphan_table_for_testing (
            id integer,
            name varchar(100)
        )
    {% endset %}

    {% set orphan_view_sql %}
        create or replace view test_dbt_orphan.orphan_view_for_testing as (
            select 1 as id, 'orphan' as name
        )
    {% endset %}

    {% set old_renamed_model_sql %}
        create table if not exists test_dbt_orphan.old_model_that_was_renamed (
            id integer,
            data varchar(100)
        )
    {% endset %}

    {% do run_query(orphan_table_sql) %}
    {{ log('Created orphan_table_for_testing', info=true) }}

    {% do run_query(orphan_view_sql) %}
    {{ log('Created orphan_view_for_testing', info=true) }}

    {% do run_query(old_renamed_model_sql) %}
    {{ log('Created old_model_that_was_renamed', info=true) }}

    {{ log('Orphan tables created successfully', info=true) }}
{% endmacro %}

