select 
	gsis_id as player_id,
    season,
    week
from {{ source('nflverse','roster_weekly_2022') }}