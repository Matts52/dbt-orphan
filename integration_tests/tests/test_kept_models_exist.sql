-- This test PASSES if kept models still exist after cleanup
-- Returns rows if any expected models are MISSING (which means test FAILS)

with expected_models as (
    select 'kept_model' as model_name
    union all
    select 'another_kept_model' as model_name
    union all
    -- versioned models: default _v<version> name, custom alias, and dotted version (1.5 -> 1_5)
    select 'versioned_model_v1' as model_name
    union all
    select 'versioned_model_v2' as model_name
    union all
    select 'versioned_alias_custom_v1' as model_name
    union all
    select 'versioned_alias_model_v2' as model_name
    union all
    select 'versioned_defined_in_model_v1_5' as model_name
),

actual_models as (
    select lower(table_name) as model_name
    from information_schema.tables
    where lower(table_schema) = lower('{{ target.schema }}')
)

select e.model_name as missing_model
from expected_models e
left join actual_models a on e.model_name = a.model_name
where a.model_name is null
