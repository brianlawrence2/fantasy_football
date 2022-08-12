select
    play_id,
    game_id,
    home_team,
    away_team,
    season_type,
    week,
    posteam,
    defteam,

from {{ source('nflverse','play_by_play') }}