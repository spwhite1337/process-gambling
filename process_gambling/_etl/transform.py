from process_gambling._etl.load import Load
from process_gambling._etl.helpers import TransformHelpers

from process_gambling.config import logger


class Transform(Load, TransformHelpers):


    def transform_odds(self):
        logger.info('Transforming Odds')
        conn = self.connect_to_db()

    def transform_scores(self):
        logger.info('Transforming Scores')
        conn = self.connect_to_db()
        conn.cursor().execute(f'DROP TABLE IF EXISTS SILVER_EVENT_SCORES_{self.sport};')
        conn.cursor().execute(f"""

        CREATE TABLE SILVER_EVENT_SCORES_{self.sport} AS
            SELECT DISTINCT
                lu.event_id,
                sc.team,
                sc.season,
                sc.kickoff_datetime,
                lu.team_name home_team,
                opponent away_team,
                team_score home_score,
                opponent_score away_score,
                overtime,
                week_no
            FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport} sc
            JOIN SILVER_EVENTS_LOOKUP_{self.sport} lu
              ON sc.kickoff_datetime = lu.event_start
             AND sc.season = lu.season
             AND sc.team = lu.team
            WHERE sc.is_home_team

            UNION
            SELECT
                event_id,
                team,
                season,
                kickoff_datetime,
                home_team,
                away_team,
                home_score,
                away_score,
                overtime,
                week_no
            FROM (
                -- Neither team is home-team for SuperBowl
                SELECT DISTINCT
                    lu.event_id,
                    sc.team,
                    sc.season,
                    sc.kickoff_datetime,
                    lu.team_name home_team,
                    opponent away_team,
                    team_score home_score,
                    opponent_score away_score,
                    overtime,
                    week_no,
                    MAX(sc.team) OVER(PARTITION BY lu.event_id) max_team
                FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport} sc
                JOIN SILVER_EVENTS_LOOKUP_{self.sport} lu
                  ON sc.kickoff_datetime = lu.event_start
                 AND sc.season = lu.season
                 AND sc.team = lu.team
                WHERE week_no = 'SuperBowl'
            )
            WHERE team = max_team
        ;
        """
        )

    def transform_events(self):
        logger.info('Transforming Events')
        conn = self.connect_to_db()
        conn.cursor().execute(
        f'DROP TABLE IF EXISTS SILVER_EVENTS_LOOKUP_{self.sport};'
        )
        conn.cursor().execute(f"""

        CREATE TABLE SILVER_EVENTS_LOOKUP_{self.sport} AS

        SELECT
            o.id event_id,
            s.kickoff_datetime event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) query_date,
            s.team,
            lu.sports_odds_name team_name,
            s.season
        FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport} s
        JOIN SILVER_TEAM_LOOKUPS_{self.sport} lu
          ON s.team = lu.sports_ref_name
          -- Use team-lookup to join Sports-ref events
          -- (defined as team, event-start)
          -- with odds-api team, event-start
        JOIN BRONZE_ODDSAPI_EVENTS_{self.sport} o
          ON STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) = o.query_date
         AND lu.sports_odds_name = o.home_team
        WHERE s.kickoff_datetime > DATE('2020-06-06')
        UNION
        SELECT
            o.id event_id,
            s.kickoff_datetime event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) query_date,
            s.team,
            lu.sports_odds_name team_name,
            s.season
        FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport} s
        JOIN SILVER_TEAM_LOOKUPS_{self.sport} lu
          ON s.team = lu.sports_ref_name
        JOIN BRONZE_ODDSAPI_EVENTS_{self.sport} o
          ON STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) = o.query_date
         AND lu.sports_odds_name = o.away_team
        WHERE s.kickoff_datetime > DATE('2020-06-06')

        -- Also join on Alt-names to account for
        -- changing franchises
        UNION
        SELECT
            o.id event_id,
            s.kickoff_datetime event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) query_date,
            s.team,
            lu.alt_name_1 team_name,
            s.season
        FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport} s
        JOIN SILVER_TEAM_LOOKUPS_{self.sport} lu
          ON s.team = lu.sports_ref_name
        JOIN BRONZE_ODDSAPI_EVENTS_{self.sport} o
          ON STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) = o.query_date
         AND lu.alt_name_1 = o.away_team
        WHERE s.kickoff_datetime > DATE('2020-06-06')
        UNION
        SELECT
            o.id event_id,
            s.kickoff_datetime event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) query_date,
            s.team,
            lu.alt_name_1 team_name,
            s.season
        FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport} s
        JOIN SILVER_TEAM_LOOKUPS_{self.sport} lu
          ON s.team = lu.sports_ref_name
        JOIN BRONZE_ODDSAPI_EVENTS_{self.sport} o
          ON STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) = o.query_date
         AND lu.alt_name_1 = o.home_team
        WHERE s.kickoff_datetime > DATE('2020-06-06')
        UNION
        SELECT
            o.id event_id,
            s.kickoff_datetime event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) query_date,
            s.team,
            lu.alt_name_2 team_name,
            s.season
        FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport} s
        JOIN SILVER_TEAM_LOOKUPS_{self.sport} lu
          ON s.team = lu.sports_ref_name
        JOIN BRONZE_ODDSAPI_EVENTS_{self.sport} o
          ON STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) = o.query_date
         AND lu.alt_name_2 = o.away_team
        WHERE s.kickoff_datetime > DATE('2020-06-06')
        UNION
        SELECT
            o.id event_id,
            s.kickoff_datetime event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) query_date,
            s.team,
            lu.alt_name_2 team_name,
            s.season
        FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport} s
        JOIN SILVER_TEAM_LOOKUPS_{self.sport} lu
          ON s.team = lu.sports_ref_name
        JOIN BRONZE_ODDSAPI_EVENTS_{self.sport} o
          ON STRFTIME('%Y-%m-%dT%H:%M:%SZ', s.kickoff_datetime) = o.query_date
         AND lu.alt_name_2 = o.home_team
        WHERE s.kickoff_datetime > DATE('2020-06-06')

        -- Manually input because some kickoff-dates in sports-ref
        -- Are far from those documented in the ODDs api
        -- Looks like Sports-ref is correct...
        {self.manual_imputes_events}
        ;
        """
        )
