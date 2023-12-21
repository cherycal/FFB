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
from git import Repo


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
        # Playoff processing: week 16 is folded into week 15 and weeks 17 and 18 are considered week 16
        if self.week > 15:
            self.week -= 1
        if self.week > 17:
            self.week -= 1
        self._run_it = True
        self._main_loop_sleep = 2400
        self._last_report_time = datetime.datetime.now().timestamp()
        self.logname = './logs/statslog.log'
        self.logger = tools.get_logger(logfilename=self.logname)
        self.leagues = self.get_leagues()
        self.fantasy_teams = self.get_team_abbrs()
        self.slack_alerts_channel = os.environ["SLACK_ALERTS_CHANNEL"]
        self.summary_msg = ""
        self.repo_dir = "C:\\Ubuntu\\Shared\\FFB"
        self.git_repo = Repo(self.repo_dir)

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
    def last_report_time(self):
        return self._last_report_time

    @last_report_time.getter
    def last_report_time(self):
        return self._last_report_time

    @last_report_time.setter
    def last_report_time(self, value):
        self.logger.info(f"Set last_report_time to {value}")
        self._last_report_time = value

    @property
    def main_loop_sleep(self):
        return self._main_loop_sleep

    @main_loop_sleep.setter
    def main_loop_sleep(self, value: int):
        self._main_loop_sleep = value

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
                try:
                    self.main_loop_sleep = int(text.upper()[2:])
                    self.logger.info(f"Score loop sleep set to {self.main_loop_sleep}")
                    self.push_instance.push(f"Score loop sleep set to {self.main_loop_sleep}")
                except Exception as ex:
                    print(f"Exception in self.main_loop_sleep: {ex}")
            else:
                print(f"Number not provided. Score loop sleep remains at {self.main_loop_sleep}")

    def slack_thread(self):
        slack_instance = push.Push(calling_function="FBScores")
        while True:
            update_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            slack_text = ""
            try:
                slack_text = slack_instance.read_slack()
                # self.logger.info(f"Slack text ({update_time}):{slack_text}.")
            except Exception as ex:
                print(f"Exception in slack_instance.read_slack: {ex}")
                self.logger.info(f"Exception in read_slack: {ex}")
                self.push_instance.push(f"Exception in read_slack: {ex}")
            if slack_text != "":
                self.logger.info(f"Slack text ({update_time}):{slack_text}.")
                slack_instance.push(f"Received slack request: {slack_text}")
                try:
                    self.process_slack_text(slack_text)
                except Exception as ex:
                    print(f"Exception in process_slack_text: {ex}")
                    self.logger.info(f"Exception in process_slack_text: {ex}")
                    self.push_instance.push(f"Exception in process_slack_text: {ex}")
            time.sleep(2)

    def get_leagues(self):
        return self.fdb.query(f"select leagueId, leagueAbbr, Year, my_team_id from Leagues where Year = {self.SEASON}")

    def get_matchup_schedule(self, league_id):
        url = f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/{self.SEASON}/segments/0/leagues/{league_id}?view=mMatchupScore"
        self.logger.info(f"get_matchup_schedule: {url}")
        matchup_schedule = self.request_instance.make_request(url=url)
        return matchup_schedule

    def get_scoreboard(self, league_id):
        url = (f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/"
               f"{self.SEASON}/segments/0/leagues/{league_id}?view=mScoreboard")
        self.logger.info(f"get_scoreboard: {url}")
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
        update_time = f"{datetime.datetime.now().strftime('%I:%M%p')}"
        self.logger.info(f"process_scoreboard at {update_time}")
        # summary = dict()
        # summary['update_time'] = update_time
        summary_msg = f""
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
                my_loc = ""
                my_team = ""
                if home_team_name in ['MO', 'ZONE', 'RULE']:
                    my_team = home_team_name
                    home_team_name += "**"
                    my_loc = "home"
                if away_team_name in ['MO', 'ZONE', 'RULE']:
                    my_team = away_team_name
                    away_team_name += "**"
                    my_loc = "away"
                if home_team == data['my_team_id'] or away_team == data['my_team_id']:
                    home_score = matchup['home']['totalPointsLive']
                    away_score = matchup['away']['totalPointsLive']
                    home_projected_score = round(matchup['home']['totalProjectedPointsLive'], 3)
                    away_projected_score = round(matchup['away']['totalProjectedPointsLive'], 3)
                    update_time = datetime.datetime.now().strftime("%#I:%M")
                    AMPM_flag = datetime.datetime.now().strftime('%p')
                    home_lead = ""
                    away_lead = ""
                    if home_projected_score > away_projected_score:
                        if my_loc == "home":
                            home_lead = f"Winning by {round(home_projected_score - away_projected_score, 1)}"
                        else:
                            away_lead = f"Losing by {round(home_projected_score - away_projected_score, 1)}"
                    else:
                        if my_loc == "away":
                            away_lead = f"Winning by {round(away_projected_score - home_projected_score, 1)}"
                        else:
                            home_lead = f"Losing by {round(away_projected_score - home_projected_score, 1)}"
                    msg = f"{update_time}{AMPM_flag}\tLeague: {league}\t\t\t\t\t\t\t\t\t\t\r\n\n" \
                          f"{home_team_name:<6} {home_score:>6.2f} - ( proj: {home_projected_score:>7.3f} ) {home_lead}" \
                          f"\t\t\t\t\t\r\n\n" \
                          f"{away_team_name:<6} {away_score:>6.2f} = ( proj: {away_projected_score:>7.3f} ) {away_lead}"
                    print(msg)
                    # summary[league] = f"{my_team} {home_lead} {away_lead}"
                    summary_msg += f"{my_team} {home_lead} {away_lead}, "
                    if msg != "":
                        self.push_instance.push(title="Score update",
                                                body=f"{update_time}{AMPM_flag}:",
                                                channel="scoreboard")
                        time.sleep(.5)
                        self.push_instance.push(title="Score update",
                                                body=f"{home_team_name:<6} {home_score:>6.2f} "
                                                     f"- ( proj: {home_projected_score:>7.3f} ) {home_lead}",
                                                channel="scoreboard")
                        time.sleep(.5)
                        self.push_instance.push(title="Score update",
                                                body=f"{away_team_name:<6} {away_score:>6.2f} "
                                                     f"- ( proj: {away_projected_score:>7.3f} ) {away_lead}",
                                                channel="scoreboard")
        self.summary_msg += summary_msg
        # print(summary)

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
        update_time = f"{datetime.datetime.now().strftime('%I:%M%p')}"
        self.logger.info(f"Scoreboard run at {update_time}")
        self.summary_msg = ""
        [self.process_league(league, self.week) for league in self.leagues]
        self.push_instance.push(title="Scores", body=f'----------------------------',
                                channel="scoreboard")
        self.push_instance.push(title="Scores", body=f'{update_time}: {self.summary_msg[:-2]}',
                                channel="scoreboard")
        self.git_push('score.txt', f'{update_time}: {self.summary_msg[:-2]}')
        current_time = int(datetime.datetime.now().strftime("%H%M"))
        if current_time == 1015 or current_time == 1255:
            self.run_query("select * from CurrentMatchupRosters")
        print(f"current time is {current_time}")
        if current_time > 2200:
            print("End of day")
            exit(0)

    def git_push(self, filename, text):
        pass
        # file = f"{self.repo_dir}\\site\\mobile\\{filename}"
        # with open(f'{file}', 'w') as f:
        #     f.write(f"{text}")
        #     f.close()
        #     assert not self.git_repo.bare
        #     git = self.git_repo.git
        #     git.pull()
        #     git.add(file)
        #     git.commit('-m', 'update', file)
        #     git.push()
        #     self.logger.info(f"pushed {filename} to git")

    def start(self):
        read_slack_thread = threading.Thread(target=self.slack_thread)
        read_slack_thread.start()
        print(f"process calling function = {self.push_instance.calling_function}")
        self.single_run()
        while True:
            if self.run_it:
                now = datetime.datetime.now().timestamp()
                print(f"S{int(self.main_loop_sleep + self.last_report_time - now)} ", end='')
                if datetime.datetime.now().timestamp() - self.last_report_time > self.main_loop_sleep:
                    self.single_run()
                    self.last_report_time = datetime.datetime.now().timestamp()
            time.sleep(5)


def main():
    scoreboard = Scoreboard()
    scoreboard.start()


if __name__ == "__main__":
    main()
