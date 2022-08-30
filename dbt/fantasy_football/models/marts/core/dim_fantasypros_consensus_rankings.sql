select
    fantasypros_id,
    player_name,
    position,
    team,
    rank,
    expert_consensus_rank,
    sd,
    best_rank,
    worst_rank,
    sportradar_id,
    yahoo_id,
    cbs_id,
    player_positions,
    player_short_name,
    player_owned_avg,
    pos_rank,
    season
from {{ ref('stg_fantasypros_consensus_rankings') }}