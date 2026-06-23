{% macro create_include_pattern_test_tables() %}
    {#
        Creates two tables to test include_patterns filtering:
        - incl_orphan_for_testing  (matches include pattern "incl_%", should be dropped)
        - noincl_orphan_for_testing (does NOT match, should be kept)
    #}

    {{ log('Creating include_pattern test tables...', info=true) }}

    {% set incl_sql %}
        create table if not exists test_dbt_orphan.incl_orphan_for_testing (
            id integer
        )
    {% endset %}

    {% set noincl_sql %}
        create table if not exists test_dbt_orphan.noincl_orphan_for_testing (
            id integer
        )
    {% endset %}

    {% do run_query(incl_sql) %}
    {{ log('Created incl_orphan_for_testing', info=true) }}

    {% do run_query(noincl_sql) %}
    {{ log('Created noincl_orphan_for_testing', info=true) }}

    {{ log('Include pattern test tables created successfully', info=true) }}
{% endmacro %}
