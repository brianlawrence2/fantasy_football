with 
player_game_stats as (
    select * from {{ ref('fct_player_game_stats') }}
),

games as (
    select * from {{ ref('dim_games') }}
),

grouped as (
    select
        player_id,

        count(distinct games.season) as seasons_played,
        count(distinct games.game_id) as games_played,
        sum(passing_yards) as passing_yards,
        sum(passing_air_yards) as passing_air_yards,
        sum(attempts) as attempts, 
        sum(completions) as completions,
        sum(passing_touchdowns) as passing_touchdowns,
        sum(interceptions) as interceptions,
        sum(sacks) as sacks,
        sum(QB_EFPA) as QB_EFPA,

        sum(receiving_yards) as receiving_yards,
        sum(receiving_air_yards) as receiving_air_yards,
        sum(targets) as targets,
        sum(receptions) as receptions,
        sum(receiving_touchdowns) as receiving_touchdowns,
        sum(receiving_FLEX_EFPA) as receiving_FLEX_EFPA,

        sum(rushing_yards) as rushing_yards,
        sum(rushing_attempts) as rushing_attempts,
        sum(rushing_touchdowns) as rushing_touchdowns,
        sum(rushing_FLEX_EFPA) as rushing_FLEX_EFPA,

        sum(two_point_conversions) as two_point_conversions,
        sum(fumbles) as fumbles

    from 
        player_game_stats
        inner join games on player_game_stats.game_id = games.game_id
    group by 1
),

calculate_points as (

    select
        player_id,

        seasons_played,
        games_played,
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
        {{ safe_divide('QB_EFPA','games_played') }} as QB_EFPA_per_game,

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
        {{ safe_divide('receiver_FLEX_EFPA','games_played') }} as receiver_FLEX_EPA,

        rushing_yards,
        rushing_attempts,
        rushing_touchdowns,
        {{ safe_divide('rushing_yards','rushing_attempts') }} as yards_per_rushing_attempt,
        {{ safe_divide('rushing_attempts','rushing_touchdowns') }} as rushing_attempt_to_touchdown_rate,
        {{ safe_divide('rusher_FLEX_EFPA','games_played') }} as rusher_FLEX_EPA,

        fumbles,

        {{ fantasy_points('passing_yards','passing_touchdowns','interceptions',
                          'rushing_yards','rushing_touchdowns','receptions',
                          'receiving_yards','receiving_touchdowns','two_point_conversions','fumbles') }} as fantasy_points
    from 
        grouped

),

final as (

    select
        player_id,

        seasons_played,
        games_played,
        passing_yards,
        passing_air_yards,
        attempts,
        completions,
        completion_rate,
        yards_per_attempt,
        passing_adot,
        passing_touchdowns,
        interceptions,
        sacks,
        attempt_to_touchdown_rate,
        attempt_to_interception_rate,
        passing_air_yard_rate,
        QB_EFPA,

        receiving_yards,
        receiving_air_yards,
        targets,
        receptions,
        catch_rate,
        yards_per_target,
        receiving_adot,
        receiving_air_yard_rate,
        receiving_touchdowns,
        target_to_touchdown_rate,
        receiver_FLEX_EFPA,

        rushing_yards,
        rushing_attempts,
        rushing_touchdowns,
        yards_per_rushing_attempt,
        rushing_attempt_to_touchdown_rate,
        rusher_FLEX_EFPA,

        fumbles,

        fantasy_points,
        {{ safe_divide('fantasy_points','games_played') }} fantasy_points_per_game
    from 
        calculate_points

)

select * from final