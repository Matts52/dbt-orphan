{% macro cleanup_stale_by_pg_stats(schema=target.schema, days_threshold=30, dry_run=true, exclude_patterns=[]) %}
    {{ return(adapter.dispatch('cleanup_stale_by_pg_stats', 'dbt_stale')(schema, days_threshold, dry_run, exclude_patterns)) }}
{% endmacro %}


{% macro postgres__cleanup_stale_by_pg_stats(schema, days_threshold, dry_run, exclude_patterns) %}
    {% set stale_objects_query %}
        with object_stats as (
            select
                t.table_schema,
                t.table_name,
                t.table_type,
                greatest(
                    coalesce(s.last_vacuum, '1970-01-01'::timestamp),
                    coalesce(s.last_autovacuum, '1970-01-01'::timestamp),
                    coalesce(s.last_analyze, '1970-01-01'::timestamp),
                    coalesce(s.last_autoanalyze, '1970-01-01'::timestamp)
                ) as last_activity
            from information_schema.tables t
            left join pg_stat_user_tables s
                on t.table_schema = s.schemaname
                and t.table_name = s.relname
            where t.table_schema = '{{ schema }}'
                and t.table_type in ('BASE TABLE', 'VIEW')
        )
        select
            table_schema,
            table_name,
            table_type,
            last_activity,
            extract(day from current_timestamp - last_activity)::int as days_since_activity
        from object_stats
        where last_activity < current_timestamp - interval '{{ days_threshold }} days'
            {% for pattern in exclude_patterns %}
                and table_name not like '{{ pattern }}'
            {% endfor %}
        order by last_activity asc
    {% endset %}

    {% set stale_objects = run_query(stale_objects_query) %}

    {% if stale_objects %}
        {% for row in stale_objects %}
            {% set object_type = 'VIEW' if row['table_type'] == 'VIEW' else 'TABLE' %}
            {% set full_name = row['table_schema'] ~ '.' ~ row['table_name'] %}

            {% if dry_run %}
                {{ log('[DRY RUN] Would drop ' ~ object_type ~ ' ' ~ full_name ~ ' (last activity: ' ~ row['last_activity'] ~ ', ' ~ row['days_since_activity'] ~ ' days ago)', info=true) }}
            {% else %}
                {{ log('Dropping stale ' ~ object_type ~ ' ' ~ full_name ~ ' (last activity: ' ~ row['last_activity'] ~ ', ' ~ row['days_since_activity'] ~ ' days ago)', info=true) }}
                {% set drop_statement %}
                    DROP {{ object_type }} IF EXISTS {{ full_name }} CASCADE
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
        {{ log('No stale objects found in schema ' ~ schema, info=true) }}
    {% endif %}
{% endmacro %}


{% macro default__cleanup_stale_by_pg_stats(schema, days_threshold, dry_run, exclude_patterns) %}
    {{ exceptions.raise_compiler_error("cleanup_stale_by_pg_stats is only supported for PostgreSQL.") }}
{% endmacro %}
