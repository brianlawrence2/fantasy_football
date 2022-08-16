{% set positions = ['passer','rusher','receiver'] %}

with play_by_play as (
    select * from {{ rev('stg_play_by_play') }}
),

{% for position in positions %}
{{ position }} as (
    select
        distinct 
        game_id,
        {{position}}_player_id as player_id
    from play_by_play
    where {{position}}_player_id is not null
),
{% endfor %}

unioned as (
{% for postion in positions %}
    select game_id, player_id from {{position}} 
    {% if not loop.last %}
    union 
    {% endif %}
{% endfor %}
)

select * from unioned