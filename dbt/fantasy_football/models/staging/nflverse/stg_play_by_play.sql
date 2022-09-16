select
    play_id,
    drive as drive_id,
    game_id,
    season,
    season_type,
    week,
    game_date,
    case
        when home_team = 'STL' then 'LAR'
        when home_team = 'LA' then 'LAR'
        when home_team = 'SD' then 'LAC'
        when home_team = 'OAK' then 'LV'
        else home_team
    end as home_team,
    case
        when away_team = 'STL' then 'LAR'
        when away_team = 'LA' then 'LAR'
        when away_team = 'SD' then 'LAC'
        when away_team = 'OAK' then 'LV'
        else away_team
    end as away_team,
    posteam,
    defteam,
    yardline_100,
    half_seconds_remaining,
    game_seconds_remaining,
    case when posteam = home_team then 1 else 0 end as IsHome,
    down,
    time,
    yrdln as yardline,
    ydstogo as yards_to_go,
    game_half as half,
    qtr as quarter,
    qb_dropback,
    qb_kneel,
    qb_spike,
    qb_scramble,
    pass_length,
    air_yards,
    yards_after_catch,
    passer_player_id,
    receiver_player_id,
    rusher_player_id,
    touchdown,
    play_type,
    yards_gained,
    interception,
    complete_pass,
    incomplete_pass,
    rush_attempt,
    pass_attempt,
    pass,
    rush,
    sack,
    fumble,
    fumble_lost,
    case when two_point_conv_result = 'success' then 1 else 0 end as two_point_conversions
from {{ source('nflverse','play_by_play') }}