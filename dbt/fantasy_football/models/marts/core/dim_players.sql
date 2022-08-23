with players as (
    select * from {{ ref('stg_players') }}
),

player_games as (
    select * from {{ ref('int_play_by_play__get_player_games') }}
),

player_games_summary as (
    select
        player_games.player_id,
        players.first_name,
        players.last_name,
        players.display_name,
        players.current_team_id,
        players.position,
        players.position_group,
        count(distinct player_games.game_id) career_games,
        min(player_games.game_id) first_game,
        max(player_games.game_id) last_game
    from 
        player_games
    inner join
        players on player_games.player_id = players.player_id
    group by 
        1,2,3,4,5,6,7
)

select * from player_games_summary