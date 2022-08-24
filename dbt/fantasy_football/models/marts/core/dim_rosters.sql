with
rosters as (
    select * from {{ ref('stg_rosters') }}
)

select * from rosters