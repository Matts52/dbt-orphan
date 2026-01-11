-- This model should NOT be dropped by cleanup macros
-- It exists in the dbt graph and represents a "live" model

select
    1 as id,
    'this model should survive cleanup' as description
