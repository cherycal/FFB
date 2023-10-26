__author__ = 'chance'

import datetime
import sys
import os
import threading

sys.path.append('./modules')
import time
import tools
import sqldb
import push
import espn_request
import pandas as pd
import dataframe_image as dfi



class Scoreboard:
    def __init__(self):
        self.SEASON = 2023
        self.STATS_YEAR = 2023
        self.fdb = sqldb.DB('Football.db')
        self.request_instance = espn_request.Request()
        self.request_instance.year = self.SEASON
        self.push_instance = push.Push(calling_function="FBScores")
        self.leagues = self.get_leagues()
        self.week = self.get_week(self.leagues[0]['leagueID'])
        self._run_it = False
        self._main_loop_sleep = 160
        self.logname = './logs/statslog.log'
        self.logger = tools.get_logger(logfilename=self.logname)
        self.leagues = self.get_leagues()
        self.fantasy_teams = self.get_team_abbrs()
        self.slack_alerts_channel = os.environ["SLACK_ALERTS_CHANNEL"]

    @property
    def run_it(self):
        return self._run_it

    @run_it.getter
    def run_it(self):
        return self._run_it

    @run_it.setter
    def run_it(self, value: bool):
        self.logger.info(f"Set run_it to {value}")
        self._run_it = value

    @property
    def main_loop_sleep(self):
        return self._main_loop_sleep

    @main_loop_sleep.setter
    def main_loop_sleep(self, value: int):
        self.main_loop_sleep = value

    def run_query(self, query, msg="query", channel=None):
        if not channel:
            channel = self.slack_alerts_channel
        lol = []
        index = list()
        print("Query: " + query)
        try:
            temp_db = sqldb.DB('Football.db')
            col_headers, rows = temp_db.select_w_cols(query)
            temp_db.close()
            for row in rows:
                lol.append(row)
                index.append("")

            df = pd.DataFrame(lol, columns=col_headers, index=index)
            # print(df)
            img = f"./{msg}.png"
            print(f"Upload file: {img}")
            dfi.export(df, img, table_conversion="matplotlib")
            push.push_attachment(img, channel=channel, body=query)
        except Exception as ex:
            print(f"Exception in run_query: {str(ex)}")
        return

    def process_slack_text(self, text):
        if text.upper() == "SN":
            self.run_it = True
        if text.upper() == "SF":
            self.run_it = False
        if text.upper() == "SR":
            self.single_run()
        if text.upper()[0:2] == "Q:":
            cmd = text.upper()[2:]
            print(f"{text.upper()[2:]}")
            self.run_query(cmd)
        if text.upper()[0:2] == "S:":
            if text[2:].isdigit():
                main_loop_sleep = int(text.upper()[2:])
                self.push_instance.push(f"Score loop sleep set to {self.main_loop_sleep}")
            else:
                self.push_instance.push(f"Number not provided. Score loop sleep remains at {self.main_loop_sleep}")

    def slack_thread(self):
        slack_instance = push.Push(calling_function="FBScores")
        while True:
            update_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            slack_text = slack_instance.read_slack()
            if slack_text != "":
                self.logger.info(f"Slack text ({update_time}):{slack_text}.")
                slack_instance.push(f"Received slack request: {slack_text}")
                self.process_slack_text(slack_text)
            time.sleep(5)

    def get_leagues(self):
        return self.fdb.query(f"select leagueId, leagueAbbr, Year, my_team_id from Leagues where Year = {self.SEASON}")

    def get_matchup_schedule(self, league_id):
        url = f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/{self.SEASON}/segments/0/leagues/{league_id}?view=mMatchupScore"
        matchup_schedule = self.request_instance.make_request(url=url)
        return matchup_schedule

    def get_scoreboard(self, league_id):
        url = f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/{self.SEASON}/segments/0/leagues/{league_id}?view=mScoreboard"
        scoreboard = self.request_instance.make_request(url=url)
        return scoreboard

    def get_team_abbrs(self):
        data = self.fdb.query(f"select league, team_id, team_abbrev from FantasyTeams")
        fantasy_team_map = dict()
        for row in data:
            if fantasy_team_map.get(row['league']):
                fantasy_team_map[row['league']][str(row['team_id'])] = row['team_abbrev']
            else:
                fantasy_team_map[row['league']] = dict()
                fantasy_team_map[row['league']][str(row['team_id'])] = row['team_abbrev']
        return fantasy_team_map

    def get_data(self, league, week):
        data = {'matchup_schedule': self.get_matchup_schedule(league['leagueID']),
                'scoreboard': self.get_scoreboard(league['leagueID']),
                'league_id': league['leagueID'], 'league_name': league['leagueAbbr'],
                'fantasy_teams': self.fantasy_teams,
                'year': self.SEASON, 'week': week, 'my_team_id': league['my_team_id']}
        return data

    def process_scoreboard(self, data):
        schedule = data['scoreboard']['schedule']
        for matchup in schedule:
            matchup_id = matchup['id']
            matchup_week = int((matchup_id - 1) / 5) + 1
            if data['week'] == matchup_week:
                league = data['league_name']
                home_team = matchup['home']['teamId']
                away_team = matchup['away']['teamId']
                home_team_name = self.fantasy_teams[league][str(home_team)]
                away_team_name = self.fantasy_teams[league][str(away_team)]
                if home_team_name in ['MO', 'ZONE', 'RULE']:
                    home_team_name += "**"
                if away_team_name in ['MO', 'ZONE', 'RULE']:
                    away_team_name += "**"
                if home_team == data['my_team_id'] or away_team == data['my_team_id']:
                    home_score = matchup['home']['totalPointsLive']
                    away_score = matchup['away']['totalPointsLive']
                    home_projected_score = round(matchup['home']['totalProjectedPointsLive'], 3)
                    away_projected_score = round(matchup['away']['totalProjectedPointsLive'], 3)
                    update_time = datetime.datetime.now().strftime("%#I:%M")
                    AMPM_flag = datetime.datetime.now().strftime('%p')
                    msg = f"{update_time} {AMPM_flag}\r\nLeague: {league}\n\t" \
                          f"-- {home_team_name:<7}{home_score:>6.2f}\t( proj: {home_projected_score:>7.3f} )\n\t" \
                          f"-- {away_team_name:<7}{away_score:>6.2f}\t( proj: {away_projected_score:>7.3f} )\n\n"
                    print(msg)
                    if msg != "":
                        self.push_instance.push(title="Score update", body=f'{msg}', channel="scoreboard")

    def process_data(self, data):
        schedule = data['matchup_schedule']['schedule']
        for matchup in schedule:
            league = data['league_name']
            home_team = matchup['home']['teamId']
            away_team = matchup['away']['teamId']
            home_team_name = self.fantasy_teams[league][str(home_team)]
            away_team_name = self.fantasy_teams[league][str(away_team)]
            week = matchup['matchupPeriodId']
            if week == data['week']:
                if home_team == data['my_team_id'] or away_team == data['my_team_id']:
                    home_score = matchup['home']['totalPointsLive']
                    away_score = matchup['away']['totalPointsLive']
                    update_time = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
                    msg = f"{update_time}\nLeague: {league}\n\t{home_team_name} {home_score}\n\t" \
                          f"{away_team_name} {away_score}"
                    print(msg)
                    if msg != "":
                        self.push_instance.push(title="Roster change", body=f'{msg}')

    def get_week(self, league_id):
        url = f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/{self.SEASON}/" \
              f"segments/0/leagues/{league_id}?view=mSchedule"
        data = self.request_instance.make_request(url=url)
        return data['scoringPeriodId']

    def process_league(self, league, week):
        data = self.get_data(league, week)
        ######################
        self.process_scoreboard(data)
        ######################
        data.clear()
        print("\n")
        time.sleep(4)

    def single_run(self):
        [self.process_league(league, self.week) for league in self.leagues]

    def start(self):
        read_slack_thread = threading.Thread(target=self.slack_thread)
        read_slack_thread.start()
        print(f"process calling function = {self.push_instance.calling_function}")
        while True:
            if self.run_it:
                self.logger.info("In process_loop")
                update_time = int(datetime.datetime.now().strftime("%H%M"))
                [self.process_league(league, self.week) for league in self.leagues]
                if update_time == 1015 or update_time == 1255:
                    self.run_query("select * from CurrentMatchupRosters")
                time.sleep(self.main_loop_sleep)
            else:
                time.sleep(5)


def main():
    scoreboard = Scoreboard()
    scoreboard.start()


if __name__ == "__main__":
    main()
