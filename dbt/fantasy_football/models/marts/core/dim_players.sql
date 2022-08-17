with players as (
    select * from {{ ref('stg_players') }}
),

play_by_play as (
    select * from {{ ref('stg_play_by_play') }}
),

player_games as (
    select * from {{ ref('int_play_by_play__get_player_games') }}
),

player_games_count as (
    select
        player_games.player_id,
        count(distinct player_games.game_id) career_games
    from player_games
)

joined as (
    select 
        players.display_name,
        players.position,
        player_games_count.career_games
    from players
    inner join player_games_count using(player_id)
)

select * from joined