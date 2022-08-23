with
play_by_play as (
    select * from {{ ref('stg_play_by_play') }}
),

games as (
    select 
        distinct
        game_id,
        season,
        season_type,
        week,
        game_date,
        home_team,
        away_team
    from 
        play_by_play
)

select * from games