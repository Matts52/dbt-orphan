{% macro get_orphans(database=target.database, schemas=[target.schema], exclude_patterns=[], include_patterns=[]) %}
    {{ return(adapter.dispatch('get_orphans', 'dbt_orphan')(database, schemas, exclude_patterns, include_patterns)) }}
{% endmacro %}

{% macro default__get_orphans(database, schemas, exclude_patterns, include_patterns) %}
    {# Build set of object names managed by dbt across all specified schemas #}
    {%- set dbt_objects_by_schema = {} -%}
    {%- for schema in schemas -%}
        {%- do dbt_objects_by_schema.update({schema | lower: []}) -%}
    {%- endfor -%}

    {%- set nodes_dict = graph.get('nodes', graph) -%}
    {%- for unique_id, node in nodes_dict.items() -%}
        {%- if node.resource_type in ['model', 'seed', 'snapshot'] -%}
            {%- if node.config.get('materialized', '') | lower != 'ephemeral' -%}
                {%- set node_schema = node.schema | lower -%}
                {%- if node_schema in dbt_objects_by_schema -%}
                    {%- if node.database | lower == database | lower or node.database is none -%}
                        {%- do dbt_objects_by_schema[node_schema].append((node.alias or node.name) | lower) -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {# Build a CTE with all dbt-managed objects #}
    with dbt_objects as (
        {{ dbt_orphan._dbt_objects_body(dbt_objects_by_schema) }}
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
        {% if include_patterns | length > 0 %}
            and (
                {% for pattern in include_patterns %}
                    lower(table_name) like lower('{{ pattern }}'){% if not loop.last %} or {% endif %}
                {% endfor %}
            )
        {% endif %}
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

{% macro bigquery__get_orphans(database, schemas, exclude_patterns, include_patterns) %}
    {# Build set of object names managed by dbt across all specified schemas #}
    {%- set dbt_objects_by_schema = {} -%}
    {%- for schema in schemas -%}
        {%- do dbt_objects_by_schema.update({schema | lower: []}) -%}
    {%- endfor -%}

    {%- set nodes_dict = graph.get('nodes', graph) -%}
    {%- for unique_id, node in nodes_dict.items() -%}
        {%- if node.resource_type in ['model', 'seed', 'snapshot'] -%}
            {%- if node.config.get('materialized', '') | lower != 'ephemeral' -%}
                {%- set node_schema = node.schema | lower -%}
                {%- if node_schema in dbt_objects_by_schema -%}
                    {%- if node.database | lower == database | lower or node.database is none -%}
                        {%- do dbt_objects_by_schema[node_schema].append((node.alias or node.name) | lower) -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {# Build a CTE with all dbt-managed objects #}
    with dbt_objects as (
        {{ dbt_orphan._dbt_objects_body(dbt_objects_by_schema, use_struct=true) }}
    ),

    {# BigQuery INFORMATION_SCHEMA is per-dataset, so union all across each requested schema #}
    db_objects as (
        {% for schema in schemas %}
        select
            lower(table_schema) as schema_name,
            lower(table_name) as object_name,
            table_name as original_table_name,
            table_schema as original_schema_name,
            table_type
        from {{ adapter.quote(database) }}.{{ schema }}.INFORMATION_SCHEMA.TABLES
        where table_type in ('BASE TABLE', 'VIEW')
        {% for pattern in exclude_patterns %}
            and lower(table_name) not like lower('{{ pattern }}')
        {% endfor %}
        {% if include_patterns | length > 0 %}
            and (
                {% for pattern in include_patterns %}
                    lower(table_name) like lower('{{ pattern }}'){% if not loop.last %} or {% endif %}
                {% endfor %}
            )
        {% endif %}
        {% if not loop.last %}union all{% endif %}
        {% endfor %}
    )

    select
        '{{ database }}' as database_name,
        db.original_schema_name as schema_name,
        db.original_table_name as table_name,
        db.table_type,
        '{{ database }}' || '.' || db.original_schema_name || '.' || db.original_table_name as full_name
    from db_objects db
    left join dbt_objects dbt
        on db.schema_name = dbt.schema_name
        and db.object_name = dbt.object_name
    where dbt.object_name is null
    order by db.schema_name, db.original_table_name
{% endmacro %}


{% macro snowflake__get_orphans(database, schemas, exclude_patterns, include_patterns) %}
    {# Build set of object names managed by dbt across all specified schemas #}
    {%- set dbt_objects_by_schema = {} -%}
    {%- for schema in schemas -%}
        {%- do dbt_objects_by_schema.update({schema | lower: []}) -%}
    {%- endfor -%}

    {%- set nodes_dict = graph.get('nodes', graph) -%}
    {%- for unique_id, node in nodes_dict.items() -%}
        {%- if node.resource_type in ['model', 'seed', 'snapshot'] -%}
            {%- if node.config.get('materialized', '') | lower != 'ephemeral' -%}
                {%- set node_schema = node.schema | lower -%}
                {%- if node_schema in dbt_objects_by_schema -%}
                    {%- if node.database | lower == database | lower or node.database is none -%}
                        {%- do dbt_objects_by_schema[node_schema].append((node.alias or node.name) | lower) -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {# Build a CTE with all dbt-managed objects #}
    with dbt_objects as (
        {{ dbt_orphan._dbt_objects_body(dbt_objects_by_schema) }}
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
        {% if include_patterns | length > 0 %}
            and (
                {% for pattern in include_patterns %}
                    lower(table_name) like lower('{{ pattern }}'){% if not loop.last %} or {% endif %}
                {% endfor %}
            )
        {% endif %}
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

{% macro redshift__get_orphans(database, schemas, exclude_patterns, include_patterns) %}
    {# Build set of object names managed by dbt across all specified schemas #}
    {%- set dbt_objects_by_schema = {} -%}
    {%- for schema in schemas -%}
        {%- do dbt_objects_by_schema.update({schema | lower: []}) -%}
    {%- endfor -%}

    {%- set nodes_dict = graph.get('nodes', graph) -%}
    {%- for unique_id, node in nodes_dict.items() -%}
        {%- if node.resource_type in ['model', 'seed', 'snapshot'] -%}
            {%- if node.config.get('materialized', '') | lower != 'ephemeral' -%}
                {%- set node_schema = node.schema | lower -%}
                {%- if node_schema in dbt_objects_by_schema -%}
                    {%- if node.database | lower == database | lower or node.database is none -%}
                        {%- do dbt_objects_by_schema[node_schema].append(node.alias | lower) -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {# Build a CTE with all dbt-managed objects #}
    with dbt_objects as (
        {{ dbt_orphan._dbt_objects_body(dbt_objects_by_schema) }}
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
        {% if include_patterns | length > 0 %}
            and (
                {% for pattern in include_patterns %}
                    lower(table_name) like lower('{{ pattern }}'){% if not loop.last %} or {% endif %}
                {% endfor %}
            )
        {% endif %}
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

{# Renders the body of the dbt_objects CTE as a single VALUES list (or UNNEST of structs on BigQuery)
   instead of one `select ... union all` branch per object, which does not scale on large projects.
   Very large lists are split into chunks of `chunk_size` rows joined with union all. #}
{% macro _dbt_objects_body(dbt_objects_by_schema, use_struct=false, chunk_size=5000) %}
    {%- set rows = [] -%}
    {%- for schema, objects in dbt_objects_by_schema.items() -%}
        {%- for obj in objects -%}
            {%- if use_struct -%}
                {%- do rows.append("struct('" ~ schema ~ "' as schema_name, '" ~ obj ~ "' as object_name)") -%}
            {%- else -%}
                {%- do rows.append("('" ~ schema ~ "', '" ~ obj ~ "')") -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}

    {%- if rows | length == 0 -%}
        {{ return('select null as schema_name, null as object_name where false') }}
    {%- endif -%}

    {%- set selects = [] -%}
    {%- for start in range(0, rows | length, chunk_size) -%}
        {%- set chunk = rows[start:start + chunk_size] | join(', ') -%}
        {%- if use_struct -%}
            {%- do selects.append('select schema_name, object_name from unnest([' ~ chunk ~ '])') -%}
        {%- else -%}
            {%- do selects.append('select * from (values ' ~ chunk ~ ') as v(schema_name, object_name)') -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(selects | join(' union all ')) }}
{% endmacro %}
