select 
	gsis_id player_id,
	display_name,
	first_name,
	last_name,
	current_team_id,
	position,
	position_group
from {{ source('nflverse','players') }}
limit 10