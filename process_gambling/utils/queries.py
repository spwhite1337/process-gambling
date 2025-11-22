
queries = {
    'training': """
SELECT
    event_id,
    team,
    season,
    team_name,
    opponent,
    is_home,
    week_no,
    event_start,
    overtime,
    team_score,
    opponent_score,
    team_score > opponent_score team_win,
    team_score - opponent_score team_margin,
    ABS(team_score - opponent_score) team_margin_abs,
    (team_score + team_spread) > opponent_score team_win_ats,
    (team_score + team_spread) - opponent_score team_margin_ats,
    ABS( (team_score + team_spread) - opponent_score ) team_margin_ats_abs,
    (opponent_score + opponent_spread) > team_score opponent_win_ats,
    team_score + opponent_score total_score,
    (team_score + opponent_score) > over_points over_win,
    team_spread,
    ABS(team_spread) team_spread_abs,
    team_spread_price,
    1 - 1/team_spread_price team_spread_prob,
    opponent_spread,
    opponent_spread_price,
    over_points,
    over_price,
    under_points,
    under_price,
    over_points - (team_score + opponent_score) over_margin,
    ABS(over_points - (team_score + opponent_score)) over_margin_abs
FROM (
    -- Convert from home / away to team / opponent
    SELECT
        events.event_id,
        events.team,
        events.team_name,
        CASE WHEN
            events.team_name = attr.home_team
            THEN attr.away_team ELSE attr.home_team
        END opponent,
        events.team_name = attr.home_team is_home,
        -- Game attributes
        attr.week_no,
        events.event_start,
        events.season,
        attr.overtime,
        CASE WHEN events.team_name = attr.home_team
        THEN attr.home_score ELSE attr.away_score END team_score,
        CASE WHEN events.team_name = attr.home_team
        THEN attr.away_score ELSE attr.home_score END opponent_score,
        CASE WHEN events.team_name = attr.home_team
        THEN attr.home_spread ELSE attr.away_spread END team_spread,
        CASE WHEN events.team_name = attr.home_team
        THEN attr.home_spread_price ELSE attr.away_spread_price END team_spread_price,
        CASE WHEN events.team_name = attr.home_team
        THEN attr.away_spread ELSE attr.home_spread END opponent_spread,
        CASE WHEN events.team_name = attr.home_team
        THEN attr.away_spread_price ELSE attr.home_spread_price END opponent_spread_price,
        over_points,
        over_price,
        under_points,
        under_price
    FROM SILVER_EVENTS_LOOKUP_americanfootball_nfl events
    JOIN (
        -- Coalesce spreads for a given market
        SELECT
        odds.event_id,
            scores.kickoff_datetime event_start,
            scores.season,
            scores.week_no,
            scores.home_team,
            scores.away_team,
            CAST(scores.home_score AS INTEGER) home_score,
            CAST(scores.away_score AS INTEGER) away_score,
            scores.overtime,
            COALESCE(odds.home_spread_point_draftkings_db0, odds.home_spread_point_draftkings_db1) home_spread,
            COALESCE(odds.home_spread_price_draftkings_db0, odds.home_spread_price_draftkings_db1) home_spread_price,
            COALESCE(odds.away_spread_point_draftkings_db0, odds.away_spread_point_draftkings_db1) away_spread,
            COALESCE(odds.away_spread_price_draftkings_db0, odds.away_spread_price_draftkings_db1) away_spread_price,
            COALESCE(odds.over_total_point_draftkings_db0, odds.over_total_point_draftkings_db1) over_points,
            COALESCE(odds.over_total_price_draftkings_db0, odds.over_total_price_draftkings_db1) over_price,
            COALESCE(odds.under_total_point_draftkings_db0, odds.under_total_point_draftkings_db1) under_points,
            COALESCE(odds.under_total_price_draftkings_db0, odds.under_total_price_draftkings_db1) under_price
        FROM SILVER_EVENT_ODDS_americanfootball_nfl odds
        JOIN SILVER_EVENT_SCORES_americanfootball_nfl scores
          ON odds.event_id = scores.event_id
          -- Bengals / Bills in 2022 was canceled due to Damar Hamlin
        WHERE home_score != 'Canceled'
    ) attr
      ON events.event_id = attr.event_id
)
WHERE week_no NOT IN ('Wild Card', 'Division', 'Conf. Champ.', 'SuperBowl')
ORDER BY event_start, event_id, team_name
    """
}

