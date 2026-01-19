{% macro get_orphans(database=target.database, schemas=[target.schema], exclude_patterns=[]) %}
    {{ return(adapter.dispatch('get_orphans', 'dbt_orphan')(database, schemas, exclude_patterns)) }}
{% endmacro %}

{% macro default__get_orphans(database, schemas, exclude_patterns) %}
    {# Build set of object names managed by dbt across all specified schemas #}
    {% set dbt_objects_by_schema = {} %}
    {% for schema in schemas %}
        {% do dbt_objects_by_schema.update({schema | lower: []}) %}
    {% endfor %}

    {% set nodes_dict = graph.get('nodes', graph) %}
    {% for unique_id, node in nodes_dict.items() %}
        {% if node.resource_type in ['model', 'seed', 'snapshot'] %}
            {% set node_schema = node.schema | lower %}
            {% if node_schema in dbt_objects_by_schema %}
                {% if node.database | lower == database | lower or node.database is none %}
                    {% do dbt_objects_by_schema[node_schema].append(node.name | lower) %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Build a CTE with all dbt-managed objects #}
    with dbt_objects as (
        {% set dbt_object_rows = [] %}
        {% for schema, objects in dbt_objects_by_schema.items() %}
            {% for obj in objects %}
                {% do dbt_object_rows.append("select '" ~ schema ~ "' as schema_name, '" ~ obj ~ "' as object_name") %}
            {% endfor %}
        {% endfor %}
        {% if dbt_object_rows | length > 0 %}
            {{ dbt_object_rows | join(' union all ') }}
        {% else %}
            select null as schema_name, null as object_name where false
        {% endif %}
    ),

    db_objects as (
        select
            lower(table_schema) as schema_name,
            lower(table_name) as object_name,
            table_name as original_table_name,
            table_schema as original_schema_name,
            table_type
        from information_schema.tables
        where lower(table_schema) in (
            {% for schema in schemas %}
                lower('{{ schema }}'){% if not loop.last %}, {% endif %}
            {% endfor %}
        )
        and table_type in ('BASE TABLE', 'VIEW')
        {% for pattern in exclude_patterns %}
            and lower(table_name) not like lower('{{ pattern }}')
        {% endfor %}
    )

    select
        '{{ database }}' as database_name,
        db.original_schema_name as schema_name,
        db.original_table_name as table_name,
        db.table_type,
        db.schema_name || '.' || db.original_table_name as full_name
    from db_objects db
    left join dbt_objects dbt
        on db.schema_name = dbt.schema_name
        and db.object_name = dbt.object_name
    where dbt.object_name is null
    order by db.schema_name, db.original_table_name
{% endmacro %}


{% macro snowflake__get_orphans(database, schemas, exclude_patterns) %}
    {# Build set of object names managed by dbt across all specified schemas #}
    {% set dbt_objects_by_schema = {} %}
    {% for schema in schemas %}
        {% do dbt_objects_by_schema.update({schema | lower: []}) %}
    {% endfor %}

    {% set nodes_dict = graph.get('nodes', graph) %}
    {% for unique_id, node in nodes_dict.items() %}
        {% if node.resource_type in ['model', 'seed', 'snapshot'] %}
            {% set node_schema = node.schema | lower %}
            {% if node_schema in dbt_objects_by_schema %}
                {% if node.database | lower == database | lower or node.database is none %}
                    {% do dbt_objects_by_schema[node_schema].append(node.name | lower) %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Build a CTE with all dbt-managed objects #}
    with dbt_objects as (
        {% set dbt_object_rows = [] %}
        {% for schema, objects in dbt_objects_by_schema.items() %}
            {% for obj in objects %}
                {% do dbt_object_rows.append("select '" ~ schema ~ "' as schema_name, '" ~ obj ~ "' as object_name") %}
            {% endfor %}
        {% endfor %}
        {% if dbt_object_rows | length > 0 %}
            {{ dbt_object_rows | join(' union all ') }}
        {% else %}
            select null as schema_name, null as object_name where false
        {% endif %}
    ),

    db_objects as (
        select
            lower(table_schema) as schema_name,
            lower(table_name) as object_name,
            table_name as original_table_name,
            table_schema as original_schema_name,
            table_type
        from {{ database }}.information_schema.tables
        where lower(table_schema) in (
            {% for schema in schemas %}
                lower('{{ schema }}'){% if not loop.last %}, {% endif %}
            {% endfor %}
        )
        and table_type in ('BASE TABLE', 'VIEW')
        {% for pattern in exclude_patterns %}
            and lower(table_name) not like lower('{{ pattern }}')
        {% endfor %}
    )

    select
        '{{ database }}' as database_name,
        db.original_schema_name as schema_name,
        db.original_table_name as table_name,
        db.table_type,
        db.original_schema_name || '.' || db.original_table_name as full_name
    from db_objects db
    left join dbt_objects dbt
        on db.schema_name = dbt.schema_name
        and db.object_name = dbt.object_name
    where dbt.object_name is null
    order by db.schema_name, db.original_table_name
{% endmacro %}
