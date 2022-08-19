{% set positions = ['passer','receiver','rusher'] %}

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
    group by player_games.player_id
),

{% for position in positions %}
{{position}}_career_stats as (
    select
        play_by_play.{{position}}_player_id as player_id,
        sum(yards_gained) total_yards_gained,
	    sum(case when pass = 1 and rush = 0 then yards_gained else 0 end) as
            {% if position == 'passer' %} passing_yards_gained {% else %} receiving_yards_gained {% endif %}, 
        {% if position == 'passer' %} 0 as receiving_yards_gained {% else %} 0 as passing_yards_gained {% endif %},
	    sum(case when rush = 1 and pass = 0 then yards_gained else 0 end) rushing_yards_gained,
	    sum(touchdown) as touchdowns,
	    sum(complete_pass) as 
            {% if position == 'passer' %} completions {% else %} receptions {% endif %},
        {% if position == 'passer' %} 0 as receptions {% else %} 0 as completions {% endif %},
        sum(complete_pass + incomplete_pass) as 
            {% if position == 'passer' %} attempts {% else %} targets {% endif %},
        {% if position == 'passer' %} 0 as targets {% else %} 0 as attempts {% endif %},
	    sum(interception) as interceptions,
	    sum(air_yards) as 
            {% if position == 'passer' %} passing_air_yards {% else %} receiving_air_yards {% endif %},
        {% if position == 'passer' %} 0 as receiving_air_yards {% else %} 0 as passing_air_yards {% endif %},
	    sum(rush_attempt) as rushes
    from play_by_play
    where play_by_play.season_type = 'REG'
    and (play_by_play.pass = 1 or play_by_play.rush = 1)
    group by play_by_play.{{position}}_player_id
),
{% endfor %}

player_career_stats as (
    {% for position in positions %}
        select
            player_id,
            total_yards_gained,
            passing_yards_gained,
            receiving_yards_gained,
            rushing_yards_gained,
            touchdowns,
            attempts,
            targets,
            completions,
            receptions,
            interceptions,
            passing_air_yards,
            receiving_air_yards,
            rushes
        from {{position}}_career_stats

        {% if not loop.last %} union distinct {% endif %}
    {% endfor %}
),

player_career_stats_rollup as (
    select
        player_id,
        sum(total_yards_gained) as total_yards_gained,
        sum(passing_yards_gained) as passing_yards_gained,
        sum(receiving_yards_gained) as receiving_yards_gained,
        sum(rushing_yards_gained) as rushing_yards_gained,
        sum(touchdowns) as touchdowns,
        sum(attempts) as attempts,
        sum(targets) as targets,
        sum(completions) as completions,
        sum(receptions) as receptions,
        sum(interceptions) as interceptions,
        sum(passing_air_yards) as passing_air_yards,
        sum(receiving_air_yards) as receiving_air_yards,
        sum(rushes) as rushes
    from player_career_stats
    group by player_id
),

joined as (
    select 
        players.player_id,
        players.display_name,
        players.position,
        player_games_count.career_games,
        player_career_stats_rollup.total_yards_gained,
        player_career_stats_rollup.passing_yards_gained,
        player_career_stats_rollup.receiving_yards_gained,
        player_career_stats_rollup.rushing_yards_gained,
        player_career_stats_rollup.touchdowns,
        player_career_stats_rollup.attempts,
        player_career_stats_rollup.targets,
        player_career_stats_rollup.completions,
        player_career_stats_rollup.receptions,
        player_career_stats_rollup.interceptions,
        player_career_stats_rollup.passing_air_yards,
        player_career_stats_rollup.receiving_air_yards,
        player_career_stats_rollup.rushes
    from players
    inner join player_games_count using(player_id)
    left join player_career_stats_rollup using(player_id)
),

final as (
    select
        player_id,
        display_name,
        position,
        total_yards_gained,
        attempts,
        completions,
        {{ safe_divide('completions', 'attempts') }} as completion_rate,
        passing_yards_gained,
        passing_air_yards,
        {{ safe_divide('passing_air_yards', 'attempts') }} as passing_depth_of_target,
        {{ safe_divide('passing_yards_gained', 'attempts') }} as yards_per_attempt,
        touchdowns,
        interceptions,
        {{ safe_divide('touchdowns', 'interceptions') }} as touchdown_to_interception_ratio,
        {{ safe_divide('attempts', 'touchdowns') }} as attempt_to_touchdown_rate,
        {{ safe_divide('attempts', 'interceptions') }} as attempt_to_interception_rate,
        targets,
        receptions,
        receiving_yards_gained,
        receiving_air_yards,
        {{ safe_divide('receiving_yards_gained', 'receptions') }} as yards_per_reception,
        {{ safe_divide('receiving_yards_gained', 'targets') }} as yards_per_target,
        {{ safe_divide('receptions', 'targets') }} catch_rate,
        {{ safe_divide('receiving_air_yards', 'targets') }} receiving_depth_of_target
    from joined 
)

select * from final