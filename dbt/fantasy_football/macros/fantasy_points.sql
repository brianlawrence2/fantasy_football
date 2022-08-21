{% macro fantasy_points(passing_yards,passing_touchdowns,interceptions,
                        rushing_yards,rushing_touchdowns,receptions,
                        receiving_yards,receiving_touchdowns,two_point_conversions,fumbles) %}
    (ifnull({{passing_yards}},0) * 0.04) + 
    (ifnull({{passing_touchdowns}},0) * 6) + 
    (ifnull({{interceptions}},0) * -2) + 
    (ifnull({{rushing_yards}},0) * 0.1) +
    (ifnull({{rushing_touchdowns}},0) * 6) +
    (ifnull({{receptions}},0)) +
    (ifnull({{receiving_yards}},0) * 0.1) +
    (ifnull({{receiving_touchdowns}},0) * 6) +
    (ifnull({{two_point_conversions}},0) * 2) +
    (ifnull({{fumbles}},0) * -2)
{% endmacro %}