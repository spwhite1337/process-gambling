from process_gambling._etl.load import Load


class Transform(Load):

    def transform_scores(self):
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
        UNION
        SELECT
            '81c8578ea74ac6db74ba12d9f694e9e2' event_id,
            '2020-10-04 17:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2020-10-04 17:00:00') query_date,
            'chi' team,
            'Chicago Bears' team_name,
            2020 season
        UNION
        SELECT
            '81c8578ea74ac6db74ba12d9f694e9e2' event_id,
            '2020-10-04 17:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2020-10-04 17:00:00') query_date,
            'clt' team,
            'Indianapolis Colts' team_name,
            2020 season
        UNION
        SELECT
            '4f9e131b7dca1395f726a9d8c14bed7a' event_id,
            '2020-10-05 23:05:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2020-10-05 23:05:00') query_date,
            'kan' team,
            'Kansas City Chiefs' team_name,
            2020 season
        UNION
        SELECT
            '4f9e131b7dca1395f726a9d8c14bed7a' event_id,
            '2020-10-05 23:05:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2020-10-05 23:05:00') query_date,
            'nwe' team,
            'New England Patriots' team_name,
            2020 season
        UNION
        SELECT
            '1c49d40e4090e0eb9c4d0cab2c6fc272' event_id,
            '2021-12-05 21:25:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-05 21:25:00') query_date,
            'ram' team,
            'Los Angeles Rams',
            2021 season
        UNION
        SELECT
            '1c49d40e4090e0eb9c4d0cab2c6fc272' event_id,
            '2021-12-05 21:25:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-05 21:25:00') query_date,
            'jax' team,
            'Jacksonville Jaguars',
            2021 season
        UNION
        SELECT
            'd1083bd9225f59b8444e7c00426eeed4' event_id,
            '2021-12-05 21:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-05 21:00:00') query_date,
            'sea' team,
            'Seattle Seahawks' team_name,
            2021 season
        UNION
        SELECT
            'd1083bd9225f59b8444e7c00426eeed4' event_id,
            '2021-12-05 21:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-05 21:00:00') query_date,
            'sfo' team,
            'San Francisco 49ers' team_name,
            2021 season
        UNION
        SELECT
            '9766c28eb9d0ba3b1afe1d9ec2cb00ae' event_id,
            '2021-12-12 18:00:00' kickoff_datetime,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-12 18:00:00') query_date,
            'cin' team,
            'Cincinnati Bengals' team_name,
            2021 season
        UNION
        SELECT
            '9766c28eb9d0ba3b1afe1d9ec2cb00ae' event_id,
            '2021-12-12 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-12 18:00:00') query_date,
            'sfo' team,
            'San Francisco 49ers' team_name,
            2021 season
        UNION
        SELECT
            '45a9db10794fbfb4a4300351a1f2a9d7' event_id,
            '2022-01-02 21:25:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-02 21:25:00') query_date,
            'rav' team,
            'Baltimore Ravens' team_name,
            2021 season
        UNION
        SELECT
            '45a9db10794fbfb4a4300351a1f2a9d7' event_id,
            '2021-12-12 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-12 18:00:00') query_date,
            'ram' team,
            'Los Angeles Rams' team_name,
            2021 season
        UNION
        SELECT
            'ec40d8338952a9798cd12b44c90d1db4' event_id,
            '2022-01-02 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-02 18:00:00') query_date,
            'nor' team,
            'New Orleans Saints' team_name,
            2021 season
        UNION
        SELECT
            'ec40d8338952a9798cd12b44c90d1db4' event_id,
            '2022-01-02 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-02 18:00:00') query_date,
            'car' team,
            'Carolina Panthers' team_name,
            2021 season
        UNION
        SELECT
            '14a8edc98fc58e8f6ae30aaed7b5c1fc' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'mia' team,
            'Miami Dolphins' team_name,
            2021 season
        UNION
        SELECT
            '14a8edc98fc58e8f6ae30aaed7b5c1fc' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'nwe' team,
            'New England Patriots' team_name,
            2021 season
        UNION
        SELECT
            'db65d83c1b4b4d7eed4a953ee05ff059' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'atl' team,
            'Atlanta Falcons' team_name,
            2021 season
        UNION
        SELECT
            'db65d83c1b4b4d7eed4a953ee05ff059' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'nor' team,
            'New Orleans Saints' team_name,
            2021 season
        UNION
        SELECT
            '672e109ed4d235a5502ce414277db27e' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'tam' team,
            'Tampa Bay Buccaneers' team_name,
            2021 season
        UNION
        SELECT
            '672e109ed4d235a5502ce414277db27e' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'car' team,
            'Carolina Panthers' team_name,
            2021 season
        ;
        """
        )

