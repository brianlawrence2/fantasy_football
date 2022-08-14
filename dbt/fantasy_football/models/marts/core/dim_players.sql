with players as (
    select * from {{ ref('stg_players') }}
),

play_by_play as (
    select * from {{ ref('stg_play_by_play') }}
),

passer_career_stats as (
    select 
        *
    from players
    inner join play_by_play on players.player_id = play_by_play.passer_player_id
)

select * from passer_career_stats