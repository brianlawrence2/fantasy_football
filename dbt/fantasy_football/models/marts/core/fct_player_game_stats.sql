WITH
player_games AS (
    select * from {{ ref('int_play_by_play__get_player_games') }}
),

play_by_play as (
  select * from {{ ref('stg_play_by_play') }}
),

efpa as (
  select * from {{ ref('stg_efpa') }}
),

passer_game_stats AS (
  SELECT
    pbp.passer_player_id AS player_id,
    pbp.season,
    pbp.week,
    pbp.game_id,
    SUM(CASE WHEN pbp.complete_pass = 1 or pbp.incomplete_pass = 1 THEN pbp.yards_gained ELSE 0 END ) AS passing_yards,
    SUM(pbp.air_yards) AS passing_air_yards,
    SUM(pbp.complete_pass + pbp.incomplete_pass + pbp.interception) AS attempts,
    SUM(pbp.complete_pass) AS completions,
    SUM(case when pbp.interception = 0 then pbp.touchdown else 0 end) AS passing_touchdowns,
    SUM(pbp.interception) AS interceptions,
    SUM(pbp.sack) as sacks,
    sum(pbp.fumble_lost) as passer_fumbles,
    sum(pbp.two_point_conversions) as passer_two_point_conversions,
    sum(efpa.QB_EFPA) as QB_EFPA
  FROM play_by_play AS pbp
  left join efpa on pbp.play_id = efpa.play_id
  where pbp.season_type = 'REG'
  GROUP BY 1,2,3,4
),

receiver_game_stats AS (
  SELECT
    pbp.receiver_player_id AS player_id,
    pbp.season,
    pbp.week,
    pbp.game_id,
    SUM(CASE WHEN pbp.pass = 1 AND pbp.rush = 0 and pbp.sack = 0 THEN pbp.yards_gained ELSE 0 END ) AS receiving_yards,
    SUM(pbp.air_yards) AS receiving_air_yards,
    SUM(pbp.complete_pass + pbp.incomplete_pass + pbp.interception) AS targets,
    SUM(pbp.complete_pass) AS receptions,
    SUM(pbp.touchdown) AS receiving_touchdowns,
    sum(pbp.fumble_lost) as receiver_fumbles,
    sum(pbp.two_point_conversions) as receiver_two_point_conversions,
    sum(efpa.FLEX_EFPA) as receiver_FLEX_EFPA
  FROM play_by_play AS pbp
  left join efpa on pbp.play_id = efpa.play_id
  GROUP BY 1,2,3,4
),

rusher_game_stats AS (
  SELECT
    pbp.rusher_player_id AS player_id,
    pbp.season,
    pbp.week,
    pbp.game_id,
    SUM(CASE WHEN pbp.play_type in ('run','qb_kneel') THEN pbp.yards_gained ELSE 0 END ) AS rushing_yards,
    SUM(case when pbp.play_type in ('run','qb_kneel') then 1 else 0 end) AS rushing_attempts,
    sum(pbp.touchdown) rushing_touchdowns,
    sum(pbp.fumble_lost) as rusher_fumbles,
    sum(pbp.two_point_conversions) as rusher_two_point_conversions,
    sum(efpa.FLEX_EFPA) as rusher_FLEX_EFPA
  FROM play_by_play AS pbp
  left join efpa on pbp.play_id = efpa.play_id
  GROUP BY 1,2,3,4
),

joined as (
  select
    player_games.game_id,
    player_games.player_id,

    passer_game_stats.passing_yards,
    passer_game_stats.passing_air_yards,
    passer_game_stats.attempts,
    passer_game_stats.completions,
    passer_game_stats.passing_touchdowns,
    passer_game_stats.interceptions,
    passer_game_stats.sacks,
    passer_game_stats.passer_two_point_conversions,
    passer_game_stats.QB_EFPA,

    receiver_game_stats.receiving_yards,
    receiver_game_stats.receiving_air_yards,
    receiver_game_stats.targets,
    receiver_game_stats.receptions,
    receiver_game_stats.receiving_touchdowns,
    receiver_game_stats.receiver_two_point_conversions,
    receiver_game_stats.receiver_FLEX_EFPA,

    rusher_game_stats.rushing_attempts,
    rusher_game_stats.rushing_yards,  
    rusher_game_stats.rushing_touchdowns,
    rusher_game_stats.rusher_two_point_conversions,
    rusher_game_stats.rusher_FLEX_EFPA,

    passer_two_point_conversions + receiver_two_point_conversions + rusher_two_point_conversions as two_point_conversions,
    ifnull(passer_fumbles,0) + ifnull(receiver_fumbles,0) + ifnull(rusher_fumbles,0) as fumbles
  from 
    player_games
  left join
    passer_game_stats on player_games.player_id = passer_game_stats.player_id 
      and player_games.game_id = passer_game_stats.game_id
  left join
    receiver_game_stats on player_games.player_id = receiver_game_stats.player_id
      and player_games.game_id = receiver_game_stats.game_id
  left join
    rusher_game_stats on player_games.player_id = rusher_game_stats.player_id
      and player_games.game_id = rusher_game_stats.game_id
),

final as (
    select
        player_id,
        game_id,

        passing_yards,
        passing_air_yards,
        attempts,
        completions,
        {{ safe_divide('completions','attempts') }} as completion_rate,
        {{ safe_divide('passing_yards','attempts') }} as yards_per_attempt,
        {{ safe_divide('passing_air_yards','attempts') }} as passing_adot,
        passing_touchdowns,
        interceptions,
        sacks,
        {{ safe_divide('attempts','passing_touchdowns') }} as attempt_to_touchdown_rate,
        {{ safe_divide('attempts','interceptions') }} as attempt_to_interception_rate,
        {{ safe_divide('passing_yards','passing_air_yards') }} as passing_air_yard_rate,
        QB_EFPA,

        receiving_yards,
        receiving_air_yards,
        targets,
        receptions,
        {{ safe_divide('receptions','targets') }} as catch_rate,
        {{ safe_divide('receiving_yards','targets') }} as yards_per_target,
        {{ safe_divide('receiving_air_yards','targets') }} as receiving_adot,
        {{ safe_divide('receiving_yards','receiving_air_yards') }} as receiving_air_yard_rate,
        receiving_touchdowns,
        {{ safe_divide('targets','receiving_touchdowns') }} as target_to_touchdown_rate,
        receiver_FLEX_EFPA,

        rushing_yards,
        rushing_attempts,
        rushing_touchdowns,
        rusher_FLEX_EFPA,

        two_point_conversions,
        fumbles,

        {{ safe_divide('rushing_yards','rushing_attempts') }} as yards_per_rushing_attempt,
        {{ safe_divide('rushing_attempts','rushing_touchdowns') }} as rushing_attempt_to_touchdown_rate,

        {{ fantasy_points('passing_yards','passing_touchdowns','interceptions',
                          'rushing_yards','rushing_touchdowns','receptions',
                          'receiving_yards','receiving_touchdowns','two_point_conversions','fumbles') }} as fantasy_points
    from 
        joined
)

select * from final