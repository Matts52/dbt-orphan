{% macro cleanup_stale_by_info_schema(database=target.database, schema=target.schema, days_threshold=30, dry_run=true, exclude_patterns=[]) %}
    {{ return(adapter.dispatch('cleanup_stale_by_info_schema', 'dbt_stale')(database, schema, days_threshold, dry_run, exclude_patterns)) }}
{% endmacro %}


{% macro snowflake__cleanup_stale_by_info_schema(database, schema, days_threshold, dry_run, exclude_patterns) %}
    {% set stale_objects_query %}
        select
            table_catalog,
            table_schema,
            table_name,
            table_type,
            last_altered,
            datediff('day', last_altered, current_timestamp()) as days_since_altered
        from {{ database }}.information_schema.tables
        where table_schema = '{{ schema }}'
            and table_type in ('BASE TABLE', 'VIEW')
            and last_altered < dateadd('day', -{{ days_threshold }}, current_timestamp())
            {% for pattern in exclude_patterns %}
                and table_name not like '{{ pattern }}'
            {% endfor %}
        order by last_altered asc
    {% endset %}

    {% set stale_objects = run_query(stale_objects_query) %}

    {% if stale_objects %}
        {% for row in stale_objects %}
            {% set object_type = 'VIEW' if row['TABLE_TYPE'] == 'VIEW' else 'TABLE' %}
            {% set full_name = row['TABLE_CATALOG'] ~ '.' ~ row['TABLE_SCHEMA'] ~ '.' ~ row['TABLE_NAME'] %}

            {% if dry_run %}
                {{ log('[DRY RUN] Would drop ' ~ object_type ~ ' ' ~ full_name ~ ' (last altered: ' ~ row['LAST_ALTERED'] ~ ', ' ~ row['DAYS_SINCE_ALTERED'] ~ ' days ago)', info=true) }}
            {% else %}
                {{ log('Dropping stale ' ~ object_type ~ ' ' ~ full_name ~ ' (last altered: ' ~ row['LAST_ALTERED'] ~ ', ' ~ row['DAYS_SINCE_ALTERED'] ~ ' days ago)', info=true) }}
                {% set drop_statement %}
                    DROP {{ object_type }} IF EXISTS {{ full_name }}
                {% endset %}
                {% do run_query(drop_statement) %}
            {% endif %}
        {% endfor %}

        {% if dry_run %}
            {{ log('[DRY RUN] Found ' ~ stale_objects | length ~ ' stale object(s) that would be dropped', info=true) }}
        {% else %}
            {{ log('Dropped ' ~ stale_objects | length ~ ' stale object(s)', info=true) }}
        {% endif %}
    {% else %}
        {{ log('No stale objects found in ' ~ database ~ '.' ~ schema, info=true) }}
    {% endif %}
{% endmacro %}

{% macro default__cleanup_stale_by_info_schema(database, schema, days_threshold, dry_run, exclude_patterns) %}
    {{ exceptions.raise_compiler_error("cleanup_stale_by_info_schema is not implemented for this adapter. Currently supported: Snowflake") }}
{% endmacro %}
