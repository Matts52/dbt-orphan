{{ config(materialized='ephemeral') }}
select 1 as id, 'ephemeral' as name
