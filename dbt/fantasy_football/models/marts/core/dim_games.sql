with
play_by_play as (
    select * from {{ ref('stg_play_by_play') }}
),

games as (
    select 
        game_id,
        season,
        season_type,
        week,
        game_date,
        home_team,
        away_team,
        max(total_home_score) as home_score,
        max(total_away_score) as away_score
    from 
        play_by_play
    group by
        1,2,3,4,5,6,7
),

final as (
    select
        game_id,
        season,
        week,
        season_type,
        game_date,
        home_team,
        away_team,
        home_score,
        away_score,
        home_score - away_score as spread
    from 
        games
)

select * from final