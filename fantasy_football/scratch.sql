SELECT    
  players.display_name,
  players.position_group as position,
  stats.season,
  stats.season as prior_season,
  stats.fantasy_points_per_game,
  stats.fantasy_points_per_game as prior_season_fantasy_points_per_game,
  stats.games_played,
   safe_divide(stats.passing_yards,stats.games_played) as passing_yards_per_game,
  safe_divide(stats.passing_touchdowns,stats.games_played) as passing_touchdowns_per_game,
  safe_divide(stats.interceptions,stats.games_played) as interceptions_per_game,
  safe_divide(stats.passing_air_yards,stats.games_played) as passing_air_yards_per_game,
  stats.passing_air_yard_rate,
  stats.passing_adot,
  safe_divide(stats.attempts,stats.games_played) as attempts_per_game,
  safe_divide(stats.completions,stats.games_played) as completions_per_game,
  stats.completion_rate,
  stats.attempt_to_touchdown_rate,
  stats.attempt_to_touchdown_rate,
  stats.yards_per_attempt,
   safe_divide(stats.targets,stats.games_played) as targets_per_game,
  safe_divide(stats.receptions,stats.games_played) as receptions_per_game,
  stats.target_to_touchdown_rate,
  stats.catch_rate,
  safe_divide(stats.receiving_yards,stats.games_played) as receiving_yards_per_game,
  safe_divide(stats.receiving_touchdowns,stats.games_played) as receiving_touchdowns_per_game,
  safe_divide(stats.receiving_air_yards,stats.games_played) as receiving_air_yards_per_game,
  stats.receiving_adot,
  stats.receiving_air_yard_rate,
  stats.yards_per_target,
   safe_divide(stats.rushing_attempts,stats.games_played) as rushing_attempts_per_game,
  safe_divide(stats.rushing_yards,stats.games_played) as rushing_yards_per_game,
  safe_divide(stats.rushing_touchdowns,stats.games_played) as rushing_touchdowns_per_game,
  stats.rushing_attempt_to_touchdown_rate,
  safe_divide(stats.rushing_yards,stats.rushing_attempts) as rushing_yards_per_attempt
FROM 
  `fantasyvbd.fantasy_football.dim_rosters` as rosters
inner join
  `fantasyvbd.fantasy_football.dim_players` as players 
on
  rosters.player_id = players.player_id
inner join
  `fantasyvbd.fantasy_football.agg_player_season_stats` stats
on 
  rosters.player_id = stats.player_id
and 
  rosters.season = stats.season + 1
where
  players.position_group in ('RB','WR','QB','TE')