select
    fantasypros_id,
    player_name,
    pos as position,
    team,
    rank,
    ecr as expert_consensus_rank,
    sd,
    best as best_rank,
    worst as worst_rank,
    sportradar_id,
    yahoo_id,
    cbs_id,
    player_positions,
    player_short_name,
    player_owned_avg,
    pos_rank,
    season
from {{ source('fantasypros','fantasypro_consensus_rankings') }}