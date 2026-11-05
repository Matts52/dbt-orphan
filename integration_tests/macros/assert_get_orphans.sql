{% macro assert_get_orphans(schemas=['test_dbt_orphan']) %}
    {#
        Checks get_orphans end to end. Run after create_orphan_tables and before cleanup_orphans.

        1. The object names it reports are exactly the expected orphans: the tables and views
           created outside dbt, plus the stale view of the ephemeral model. Models dbt manages
           must not appear.
        2. The compiled SQL stays compact. The macro loops over every node in the graph, and
           untrimmed Jinja tags leave blank lines behind for each one. On a large project that
           pushed the query past 1 MB, which Snowflake rejected with internal error 370001.
           The template itself never has more than two consecutive newlines, so five in a row
           can only come from a per-node loop.
    #}
    {% set sql = dbt_orphan.get_orphans(schemas=schemas) %}

    {% if modules.re.search('(\n[ \t]*){5,}', sql) %}
        {% do exceptions.raise_compiler_error('get_orphans emitted a run of blank lines; per-node Jinja tags need whitespace control ({%- -%})') %}
    {% endif %}

    {% set orphans = [] %}
    {% for row in run_query(sql).rows %}
        {% do orphans.append(row['table_name'] | lower) %}
    {% endfor %}

    {% set expected = [
        'ephemeral_model',
        'old_model_that_was_renamed',
        'orphan_table_for_testing',
        'orphan_view_for_testing'
    ] %}

    {% if orphans | sort != expected %}
        {% do exceptions.raise_compiler_error('get_orphans returned ' ~ (orphans | sort) ~ ', expected ' ~ expected) %}
    {% endif %}

    {{ log('get_orphans returned the expected orphans and compact SQL', info=true) }}
{% endmacro %}
