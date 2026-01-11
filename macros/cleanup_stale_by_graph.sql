{% macro cleanup_stale_by_graph(database=target.database, schema=target.schema, dry_run=true, exclude_patterns=[]) %}

    {# Build set of object names managed by dbt in this schema #}
    {% set dbt_objects = [] %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type in ['model', 'seed', 'snapshot'] %}
            {% if node.schema | lower == schema | lower %}
                {% if node.database | lower == database | lower or node.database is none %}
                    {% do dbt_objects.append(node.name | lower) %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {{ return(adapter.dispatch('cleanup_stale_by_graph', 'dbt_stale')(database, schema, dry_run, exclude_patterns, dbt_objects)) }}
{% endmacro %}

{% macro default__cleanup_stale_by_graph(database, schema, dry_run, exclude_patterns, dbt_objects) %}
    {% set db_objects_query %}
        select
            table_name,
            table_type
        from information_schema.tables
        where lower(table_schema) = lower('{{ schema }}')
            and table_type in ('BASE TABLE', 'VIEW')
            {% for pattern in exclude_patterns %}
                and lower(table_name) not like lower('{{ pattern }}')
            {% endfor %}
        order by table_name
    {% endset %}

    {% set db_objects = run_query(db_objects_query) %}

    {% set orphaned_count = [] %}
    {% if db_objects %}
        {% for row in db_objects %}
            {% set obj_name = row['table_name'] | lower %}
            {% if obj_name not in dbt_objects %}
                {% set object_type = 'VIEW' if row['table_type'] == 'VIEW' else 'TABLE' %}
                {% set full_name = schema ~ '.' ~ row['table_name'] %}

                {% if dry_run %}
                    {{ log('[DRY RUN] Would drop ' ~ object_type ~ ' ' ~ full_name ~ ' (not in dbt graph)', info=true) }}
                {% else %}
                    {{ log('Dropping orphaned ' ~ object_type ~ ' ' ~ full_name ~ ' (not in dbt graph)', info=true) }}
                    {% set drop_statement %}
                        DROP {{ object_type }} IF EXISTS {{ full_name }}
                    {% endset %}
                    {% do run_query(drop_statement) %}
                {% endif %}
                {% do orphaned_count.append(1) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% if orphaned_count | length > 0 %}
        {% if dry_run %}
            {{ log('[DRY RUN] Found ' ~ orphaned_count | length ~ ' orphaned object(s) that would be dropped', info=true) }}
        {% else %}
            {{ log('Dropped ' ~ orphaned_count | length ~ ' orphaned object(s)', info=true) }}
        {% endif %}
    {% else %}
        {{ log('No orphaned objects found in ' ~ database ~ '.' ~ schema, info=true) }}
    {% endif %}
{% endmacro %}


{% macro snowflake__cleanup_stale_by_graph(database, schema, dry_run, exclude_patterns, dbt_objects) %}
    {% set db_objects_query %}
        select
            table_name,
            table_type
        from {{ database }}.information_schema.tables
        where lower(table_schema) = lower('{{ schema }}')
            and table_type in ('BASE TABLE', 'VIEW')
            {% for pattern in exclude_patterns %}
                and lower(table_name) not like lower('{{ pattern }}')
            {% endfor %}
        order by table_name
    {% endset %}

    {% set db_objects = run_query(db_objects_query) %}

    {% set orphaned_count = [] %}
    {% if db_objects %}
        {% for row in db_objects %}
            {% set obj_name = row['TABLE_NAME'] | lower %}
            {% if obj_name not in dbt_objects %}
                {% set object_type = 'VIEW' if row['TABLE_TYPE'] == 'VIEW' else 'TABLE' %}
                {% set full_name = database ~ '.' ~ schema ~ '.' ~ row['TABLE_NAME'] %}

                {% if dry_run %}
                    {{ log('[DRY RUN] Would drop ' ~ object_type ~ ' ' ~ full_name ~ ' (not in dbt graph)', info=true) }}
                {% else %}
                    {{ log('Dropping orphaned ' ~ object_type ~ ' ' ~ full_name ~ ' (not in dbt graph)', info=true) }}
                    {% set drop_statement %}
                        DROP {{ object_type }} IF EXISTS {{ full_name }}
                    {% endset %}
                    {% do run_query(drop_statement) %}
                {% endif %}
                {% do orphaned_count.append(1) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% if orphaned_count | length > 0 %}
        {% if dry_run %}
            {{ log('[DRY RUN] Found ' ~ orphaned_count | length ~ ' orphaned object(s) that would be dropped', info=true) }}
        {% else %}
            {{ log('Dropped ' ~ orphaned_count | length ~ ' orphaned object(s)', info=true) }}
        {% endif %}
    {% else %}
        {{ log('No orphaned objects found in ' ~ database ~ '.' ~ schema, info=true) }}
    {% endif %}
{% endmacro %}
