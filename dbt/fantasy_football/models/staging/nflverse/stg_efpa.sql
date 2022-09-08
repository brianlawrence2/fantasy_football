select
    play_id,
    game_id,
    drive_id,
    passer_player_id,
    receiver_player_id,
    rusher_player_id,
    FLEX_EFPA,
    QB_EFPA
from {{ source('nflverse','view_raw_efpa') }}