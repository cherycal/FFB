__author__ = 'chance'

import inspect
import json
import os
import pickle
import time
import urllib.request
from datetime import datetime, timedelta
from io import BytesIO
from os import path

import certifi
import dataframe_image as dfi
import pandas as pd
import pycurl

import push
# sys.path.append('./modules')
import sqldb
import tools


def get_default_position(positionID):
    switcher = {
        1: "SP",
        2: "C",
        3: "1B",
        4: "2B",
        5: "3B",
        6: "SS",
        7: "LF",
        8: "CF",
        9: "RF",
        10: "DH",
        11: "RP",
    }
    return switcher.get(positionID, "NA")


def print_calling_function():
    print('\n')
    print("Printing calling information (fantasy.py)")
    print("#############################")
    # print(str(inspect.stack()[-2].filename) + ", " + str(inspect.stack()[-2].function) +
    #      ", " + str(inspect.stack()[-2].lineno))
    print(str(inspect.stack()[1].filename) + ", " + str(inspect.stack()[1].function) +
          ", " + str(inspect.stack()[1].lineno))
    # print(str(inspect.stack()[-1].filename) + ", " + str(inspect.stack()[-1].function) +
    #      ", " + str(inspect.stack()[-1].lineno))
    print("#############################")
    return


def get_time():
    timedict = {}
    now = datetime.now()  # current date and time
    timedict['datetime'] = now.strftime("%Y%m%d%H%M%S")
    timedict['date_time'] = now.strftime("%Y%m%d-%H%M%S")
    timedict['date8'] = now.strftime("%Y%m%d")
    timedict['time'] = now.strftime("%H%M%S")
    return timedict


class Fantasy(object):

    def __init__(self, mode="PROD", caller=os.path.basename(__file__)):
        self.espn_player_json = None
        self.timedict = {}
        filename = str(inspect.stack()[-1].filename).split('/')[-1]
        logname = './logs/' + caller + '.log'
        print("Initializing Fantasy Object from " + caller)
        print("Log name " + logname)
        self.push_msg = ""
        db = 'Baseball.db'
        if mode == "TEST":
            db = 'BaseballTest.db'
        self.platform = tools.get_platform()
        if self.platform == "Windows":
            self.db = 'C:\\Ubuntu\\Shared\\data\\' + db
        elif self.platform == "linux" or self.platform == 'Linux':
            self.db = '/media/sf_Shared/data/' + db
        else:
            print("Platform " + self.platform + " not recognized in sqldb::DB. Exiting.")
            exit(-1)
        self.year = "2021"
        self.roster_year = "2021"
        self.msg = "Msg: "
        self.DB = sqldb.DB(db)
        self.teamName = self.set_ID_team_map()
        self.MLBTeamName = self.set_espn_MLB_team_map()
        self.ownerTeam = self.set_owner_team_map()
        self.position2 = self.set_espn_position_map()
        self.position = self.load_position_dict()
        self.player, self.player_team = self.set_espn_player()
        # self.espn_player_json = self.set_espn_player_json()
        self.espn_trans_ids = self.get_espn_trans_ids()
        self.leagues = self.get_leagues()
        self.active_leagues = self.get_active_leagues()
        self.default_league = self.set_espn_default_league()
        self.game_dates = self.set_game_dates()
        self.str = ""
        self.db_player_status = {}
        self.current_player_status = {}
        self.player_insert_list = list()
        self.players = {}
        self.push_msg_list = list()
        self.transactions = {}
        self.player_data_json = object()
        self.TIMEOUT = 30
        self.roster_lock_date = None
        self.roster_lock_time = None
        self.set_roster_lock_time()
        now = datetime.now()  # current date and time
        self.date = now.strftime("%Y%m%d")
        self.statcast_date = now.strftime("%Y-%m-%d")
        self.hhmmss = self.get_hhmmss()
        self.logger_instance = tools.get_logger(logfilename=f'{logname}_{self.date}')
        self.logger_instance.debug(f'Initializing fantasy object: {caller}')
        self.push_instance = push.Push(self.logger_instance)

    def post_log_msg(self, msg):
        self.logger_instance.debug(msg)

    def logger_exception(self, msg):
        self.logger_instance.exception(msg)

    def logger_debug(self, msg):
        self.logger_instance.debug(msg)

    def logger_info(self, msg):
        self.logger_instance.info(msg)

    def logger_warning(self, msg):
        self.logger_instance.warning(msg)

    def get_date8(self):
        now = datetime.now()
        self.date = now.strftime("%Y%m%d")
        return self.date

    def get_hhmmss(self):
        now = datetime.now()  # current date and time
        self.hhmmss = now.strftime("%H%M%S")
        return self.hhmmss

    def get_db(self):
        return self.DB

    def get_statid_dict(self, verbose=True):
        return_dict = {}
        rows = self.DB.select("SELECT statid, statabbr from ESPNStatIds", verbose)
        for row in rows:
            return_dict[row[0]] = row[1]
        return return_dict

    def get_start_scoring_period_dict(self, verbose=True):
        return_dict = {}
        rows = self.DB.select("SELECT year, start_date from ESPNScoringPeriodStart", verbose)
        for row in rows:
            return_dict[row[0]] = row[1]
        return return_dict

    def get_date_from_scoring_id(self, year, scoring_id, verbose=False):
        scoring_period_dict = self.get_start_scoring_period_dict(verbose)
        start_date = scoring_period_dict[year]
        # print(str(start_date))
        d = datetime.strptime(str(start_date), '%Y%m%d')
        date_ = d + timedelta(days=scoring_id - 1)
        date8 = date_.strftime('%Y%m%d')
        return date8

    ##########################################################################

    class Player:
        def __init__(self, espnid_):
            now = datetime.now()  # current date and time
            self.date = now.strftime("%Y%m%d")
            self.time = now.strftime("%Y%m%d-%H%M%S")
            self.espnid = espnid_
            self.name = "NA"
            self.injuryStatus = "NA"
            self.throws = "NA"
            self.bats = "NA"
            self.primaryPosition = "NA"
            self.eligiblePositions = "NA"
            self.mlbTeam = "NA"
            self.auctionValueAverage = 0.0
            self.auctionValueAverageChange = 0.0
            self.averageDraftPosition = 0.0
            self.percentOwned = 0.0
            self.percentOwnedChange = 0.0
            self.percentStarted = 0.0
            self.nextStart = "NA"
            self.status = "NA"

        def get_player_data_fields(self):
            fields = list()
            fields.append(self.get_date())
            fields.append(self.get_time())
            fields.append(self.get_espnid())
            fields.append(self.get_name())
            fields.append(self.get_injuryStatus())
            fields.append(self.get_throws())
            fields.append(self.get_bats())
            fields.append(self.get_primaryPosition())
            fields.append(self.get_eligiblePositions())
            fields.append(self.get_mlbTeam())
            fields.append(self.get_auctionValueAverage())
            fields.append(self.get_auctionValueAverageChange())
            fields.append(self.get_averageDraftPosition())
            fields.append(self.get_percentOwned())
            fields.append(self.get_percentOwnedChange())
            fields.append(self.get_percentStarted())
            fields.append(self.get_nextStartId())
            fields.append(self.get_status())
            return tuple(fields)

        def get_espnid(self):
            return self.espnid

        def get_date(self):
            now = datetime.now()  # current date and time
            self.date = now.strftime("%Y%m%d")
            return self.date

        def get_time(self):
            now = datetime.now()  # current date and time
            self.time = now.strftime("%Y%m%d-%H%M%S")
            return self.time

        def get_name(self):
            return self.name

        def get_injuryStatus(self):
            return self.injuryStatus

        def get_status(self):
            return self.status

        def get_throws(self):
            return self.throws

        def get_primaryPosition(self):
            return self.primaryPosition

        def get_bats(self):
            return self.bats

        def get_eligiblePositions(self):
            return self.eligiblePositions

        def get_mlbTeam(self):
            return self.mlbTeam

        def get_auctionValueAverage(self):
            return self.auctionValueAverage

        def get_auctionValueAverageChange(self):
            return self.auctionValueAverageChange

        def get_percentStarted(self):
            return self.percentStarted

        def get_percentOwned(self):
            return self.percentOwned

        def get_percentOwnedChange(self):
            return self.percentOwnedChange

        def get_nextStartId(self):
            return self.nextStart

        def get_averageDraftPosition(self):
            return self.averageDraftPosition

        def set_name(self, name_):
            self.name = name_

        def set_start(self, start_):
            self.nextStart = start_

        def set_injuryStatus(self, injuryStatus_):
            self.injuryStatus = injuryStatus_

        def set_throws(self, throws_):
            self.throws = throws_

        def set_bats(self, bats_):
            self.bats = bats_

        def set_mlbTeam(self, mlbTeam_):
            self.mlbTeam = mlbTeam_

        def set_primaryPosition(self, primaryPosition_):
            self.primaryPosition = primaryPosition_

        def set_eligiblePositions(self, eligiblePositions_):
            self.eligiblePositions = eligiblePositions_

        def set_auctionValueAverage(self, auctionValueAverage_):
            self.auctionValueAverage = auctionValueAverage_

        def set_auctionValueAverageChange(self, auctionValueAverageChange_):
            self.auctionValueAverageChange = auctionValueAverageChange_

        def set_averageDraftPosition(self, averageDraftPosition_):
            self.averageDraftPosition = averageDraftPosition_

        def set_percentOwned(self, percentOwned_):
            self.percentOwned = percentOwned_

        def set_percentOwnedChange(self, percentOwnedChange_):
            self.percentOwnedChange = percentOwnedChange_

        def set_percentStarted(self, percentStarted_):
            self.percentStarted = percentStarted_

        def set_status(self, status_):
            self.status = status_

        def print_attrs(self):
            print(vars(self).keys())
            print(vars(self).values())

        def keys(self):
            return vars(self).keys()

        def values(self):
            return vars(self).values()

    ###########################################

    class Transaction:
        def __init__(self, espntransid_):
            self.espntransid = espntransid_
            self.update_date = 0
            self.update_time = ""
            self.update_time_hhmmss = 0
            self.fantasy_team_name = ""
            self.status = ""
            self.type = ""
            self.transid = ""
            self.from_position = ""
            self.from_team = ""
            self.player_name = ""
            self.to_position = ""
            self.to_team = ""
            self.leg_type = ""
            self.espnid = ""
            self.leagueID = ""
            self.year = "2021"
            self.hhmmss = self.get_hhmmss()

        def set_leagueID(self, leagueID):
            self.leagueID = leagueID

        def get_leagueID(self):
            return self.leagueID

        def get_espnid(self):
            return self.espnid

        def set_espnid(self, espnid):
            self.espnid = espnid

        def get_transaction_fields(self):
            fields = list()
            fields.append(self.get_espntransid())
            fields.append(self.get_update_date())
            fields.append(self.get_update_time())
            fields.append(self.get_fantasy_team_name())
            fields.append(self.get_status())
            fields.append(self.get_type())
            fields.append(self.get_transid())
            fields.append(self.get_from_position())
            fields.append(self.get_from_team())
            fields.append(self.get_player_name())
            fields.append(self.get_to_position())
            fields.append(self.get_to_team())
            fields.append(self.get_leg_type() or "NA")
            fields.append(self.get_espnid())
            return tuple(fields)

        def get_hhmmss(self):
            now = datetime.now()  # current date and time
            self.hhmmss = int(now.strftime("%H%M%S"))
            return self.hhmmss

        def set_leg_type(self, leg_type):
            self.leg_type = leg_type

        def get_leg_type(self):
            return self.leg_type

        def set_to_team(self, to_team):
            self.to_team = to_team

        def get_to_team(self):
            return self.to_team

        def set_to_position(self, to_position):
            self.to_position = to_position

        def get_to_position(self):
            return self.to_position

        def set_player_name(self, player_name):
            self.player_name = player_name

        def get_player_name(self):
            return self.player_name

        def set_from_team(self, from_team):
            self.from_team = from_team

        def get_from_team(self):
            return self.from_team

        def set_from_position(self, from_position):
            self.from_position = from_position

        def get_from_position(self):
            return self.from_position

        def set_transid(self, transid):
            self.transid = transid

        def get_transid(self):
            return self.transid

        def set_type(self, type_):
            self.type = type_

        def get_type(self):
            return self.type

        def set_status(self, status):
            self.status = status

        def get_status(self):
            return self.status

        def set_fantasy_team_name(self, name):
            self.fantasy_team_name = name

        def get_fantasy_team_name(self):
            return self.fantasy_team_name

        def set_update_time(self, update_time="", update_time_hhmmss=""):
            now = datetime.now()  # current date and time
            if update_time == "":
                update_time = now.strftime("%Y%m%d-%H%M%S.%f")
                update_time_hhmmss = now.strftime("%H%M%S")
            self.update_time = update_time
            self.update_time_hhmmss = update_time_hhmmss

        def get_update_time_hhmmss(self):
            return self.update_time_hhmmss

        def get_update_time(self):
            return self.update_time

        def get_espntransid(self):
            return self.espntransid

        def set_update_date(self, update_date=""):
            if update_date == "":
                now = datetime.now()  # current date and time
                update_date = now.strftime("%Y%m%d")
            self.update_date = update_date

        def get_update_date(self):
            return self.update_date

        def print_attrs(self):
            print(vars(self).keys())
            print(vars(self).values())

        def keys(self):
            return vars(self).keys()

        def values(self):
            return vars(self).values()

    ###########################################

    def get_roster_lock_time(self):
        return self.roster_lock_time

    def check_roster_lock_time(self):
        print(f'Roster lock date: {self.get_roster_lock_date()}, today: {self.get_date8()}')
        if self.get_roster_lock_date() and int(self.get_roster_lock_date()) < int(self.get_date8()):
            self.set_roster_lock_time()
        else:
            pass
        # print(f'Roster lock time has been set to {self.get_roster_lock_time()}')

    def get_roster_lock_date(self):
        return self.roster_lock_date

    def set_roster_lock_time(self):
        try:
            url_name = "http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
            with urllib.request.urlopen(url_name, timeout=self.TIMEOUT) as url:
                data = json.loads(url.read().decode())
                first_game_time = 235959
                for i in data['events']:
                    date_str = str(i['date'])
                    date_str = date_str.replace("T", " ")
                    date_str = date_str.replace("Z", "")
                    datetime_object = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                    datetime_object = datetime_object - timedelta(hours=7)
                    lock_time = int(datetime.strftime(datetime_object, '%H%M%S'))
                    self.roster_lock_date = int(datetime.strftime(datetime_object, '%Y%m%d'))
                    if lock_time < first_game_time:
                        first_game_time = lock_time
                if self.roster_lock_date == self.get_date8():
                    print(f'Setting roster lock time to {first_game_time}')
                self.roster_lock_time = first_game_time
        except Exception as ex:
            print(str(ex))

    def get_time(self):
        now = datetime.now()  # current date and time
        self.timedict[datetime] = int(now.strftime("%Y%m%d%H%M%S"))
        self.timedict['date_time'] = int(now.strftime("%Y%m%d%H%M%S"))
        self.timedict['date8'] = int(now.strftime("%Y%m%d"))
        self.timedict['time'] = int(now.strftime("%H%M%S"))
        return self.timedict

    def exists_player_object(self, id_):
        # print_calling_function()
        retval = False
        if self.players.get(str(id_)):
            retval = True
        return retval

    def get_player_object(self, id_):
        # print_calling_function()
        player = object()
        if self.players.get(str(id_)):
            player = self.players[str(id_)]
        return player

    def team_name(self, proTeamID):
        team = "NA"
        if str(proTeamID) in self.MLBTeamName:
            team = self.MLBTeamName[str(proTeamID)]
        return team

    def get_position(self, positionID):
        pos = "NA"
        if str(positionID) in self.position2:
            pos = self.position2[str(positionID)]
        return pos

    def league_standings(self):
        leagueID = self.default_league
        url_name = "http://fantasy.espn.com/apis/v3/games/flb/seasons/" + self.year + \
                   "/segments/0/leagues/" + \
                   str(leagueID) + "?view=standings"
        self.logger_instance.debug(f'league_standings: {url_name}')
        with urllib.request.urlopen(url_name, timeout=self.TIMEOUT) as url:
            json_object = json.loads(url.read().decode())
        return json_object

    def get_espn_player_json(self):
        return self.espn_player_json

    def espn_player_name(self):
        return self.player

    def espn_player_mlb_team(self):
        return self.player_team

    def espn_position_map(self):
        return self.position2

    def owner_team_map(self):
        return self.ownerTeam

    def mlb_team_name_map(self):
        return self.MLBTeamName

    def team_name_map(self):
        return self.teamName

    def DB(self):
        return self.DB

    def push_instance(self):
        return self.push_instance

    def get_msg(self):
        return self.msg

    def set_msg(self, msg):
        self.msg = msg
        return 0

    def append_msg(self, msg):
        self.msg += msg
        return 0

    def set_game_dates(self):
        game_dates = {}
        query = "select GameID, Date from ESPNGameData"
        rows = self.DB.query(query)
        for row in rows:
            game_dates[str(row['GameID'])] = str(row['Date'])
        return game_dates

    def get_active_leagues(self):
        leagues = {}
        query = "select distinct LeagueID from ESPNLeagues where Active = 'True'"
        rows = self.DB.query(query)
        for row in rows:
            leagues[str(row['LeagueID'])] = str(row['LeagueID'])[0]
        return leagues

    def set_espn_MLB_team_map(self):
        teamName = {}
        query = "select MLBTeamID, MLBTeam from ESPNMLBTeams"
        rows = self.DB.query(query)
        for row in rows:
            teamName[str(row['MLBTeamID'])] = str(row['MLBTeam'])
        return teamName

    def set_espn_player(self):
        player = {}
        player_team = {}
        query = "select espnid, name, mlbTeam from ESPNPlayerDataCurrent"
        rows = self.DB.query(query)
        for row in rows:
            player[str(row['espnid'])] = str(row['name'])
            player_team[str(row['espnid'])] = str(row['mlbTeam'])
        return player, player_team

    def set_espn_player_json(self):
        print_calling_function()
        leagueID = self.default_league
        url_name = "http://fantasy.espn.com/apis/v3/games/flb/seasons/" + self.year + \
                   "/segments/0/leagues/" + \
                   str(leagueID) + "?view=kona_playercard"
        self.logger_instance.debug(f'set_espn_player_json: {url_name}')
        with urllib.request.urlopen(url_name, timeout=self.TIMEOUT) as url:
            json_object = json.loads(url.read().decode())
        # json_formatted = json.dumps(json_object, indent=2)
        return json_object

    def get_db_player_status(self):
        return self.db_player_status

    def set_next_start(self):
        # print_calling_function()
        url_name = "https://fantasy.espn.com/apis/v3/games/flb/" \
                   "seasons/2021/segments/0/leagues/37863846?" \
                   "scoringPeriodId=20&view=kona_player_info"

        headers = ['authority: fantasy.espn.com',
                   'accept: application/json',
                   'x-fantasy-source: kona',
                   'x-fantasy-filter: {"players":{"filterStatus":{"value":["FREEAGENT","WAIVERS","ONTEAM"]},'
                   '"filterSlotIds":{"value":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]}}}']

        # print("set_next_start: " + url_name)

        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url_name)
        c.setopt(c.CONNECTTIMEOUT, self.TIMEOUT)
        c.setopt(c.HTTPHEADER, headers)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.CAINFO, certifi.where())
        c.perform()
        c.close()
        data = buffer.getvalue()

        player_data_json = json.loads(data)
        for player in player_data_json['players']:
            if player.get('player'):
                if player['player'].get('id'):
                    if self.exists_player_object(player['id']):
                        player_obj = self.get_player_object(player['id'])
                    else:
                        player_obj = self.Player(player['id'])
                    if player['player'].get('starterStatusByProGame'):
                        next_start = "NA"
                        for game in player['player']['starterStatusByProGame']:
                            if player['player']['starterStatusByProGame'][game] == 'PROBABLE':
                                if self.game_dates.get(game):
                                    if self.game_dates[game] >= self.date:
                                        if next_start == "NA" or game < next_start:
                                            next_start = game

                        if self.game_dates.get(next_start):
                            # print(player['player']['fullName'])
                            # print(player['player']['id'])
                            # print(next_start)
                            # print(self.game_dates[next_start])
                            # print("\n")
                            player_obj.set_start(next_start)

        return

    @tools.try_wrap
    def get_db_player_info(self):
        # print_calling_function()
        player_status = {}
        rows = self.DB.query("select Date, espnid, injuryStatus, status,"
                             "nextStartID from ESPNPlayerDataCurrent")
        for row in rows:
            key = str(row['espnid'])
            # key = str(row['Date']) + ':' + str(row['espnid'])
            player_status[key] = {}
            if 'injuryStatus' in player_status[key]:
                player_status[key]['injuryStatus'] = str(row['injuryStatus'])
            else:
                player_status[key]['injuryStatus'] = {}
                player_status[key]['injuryStatus'] = str(row['injuryStatus'])
            if 'status' in player_status[key]:
                player_status[key]['status'] = str(row['status'])
            else:
                player_status[key]['status'] = {}
                player_status[key]['status'] = str(row['status'])
            if 'nextStartID' in player_status[key]:
                player_status[key]['nextStartID'] = str(row['nextStartID'])
            else:
                player_status[key]['nextStartID'] = {}
                player_status[key]['nextStartID'] = str(row['nextStartID'])

        self.db_player_status = player_status.copy()

    @tools.try_wrap
    def refresh_starter_history(self):
        self.DB.cmd("INSERT INTO StarterHistory SELECT * FROM UpcomingStartsWithStats", True)

    @tools.try_wrap
    def refresh_rosters(self):
        pos_map = self.set_espn_position_map()
        now = datetime.now()
        update_date = now.strftime("%Y%m%d-%H%M%S")
        insert_list = list()
        for league in self.active_leagues:
            yr = "2021"
            addr = "https://fantasy.espn.com/apis/v3/games/flb/seasons/" + yr + \
                   "/segments/0/leagues/" + str(league) + \
                   "?view=mDraftDetail&view=mLiveScoring&view=mMatchupScore&view=mPendingTransactions&" + \
                   "view=mPositionalRatings&view=mRoster&view=mSettings&view=mTeam&view=modular&view=mNav"
            # print(addr)

            try:
                with urllib.request.urlopen(addr, timeout=self.TIMEOUT) as url:
                    data = json.loads(url.read().decode())
                    for j in data['teams']:
                        # team_id = str(j['id'])
                        # team_abbrev = str(j['abbrev'])
                        team_name = str(j['location']) + " " + str(j['nickname'])
                        for k in j['roster']['entries']:
                            league = str(league)
                            espn_id = str(k['playerId'])
                            player_full_name = k['playerPoolEntry']['player']['fullName']
                            pos = str(pos_map[str(k['lineupSlotId'])])
                            entry = (player_full_name, team_name, league, espn_id, pos, update_date, yr)
                            # print(entry)
                            # print(team_abbrev)
                            insert_list.append(entry)
            except Exception as ex:
                print(str(ex))

        count = len(insert_list)
        # print(count)
        if count > 800:
            command = "Delete from ESPNRosters"
            self.DB.delete(command)
            # print("\nDelete Rosters worked\n")
            self.DB.insert_many("ESPNRosters", insert_list)
        # print("\nInsert Rosters worked\n")

    def get_current_player_status(self):
        return self.current_player_status

    @tools.try_wrap
    def get_espn_player_info(self):
        # print_calling_function()
        player_status = {}
        # Get next start info
        self.set_next_start()
        self.get_player_data_json()
        espn_player_json = self.player_data_json
        insert_many_list = list()
        for player in espn_player_json['players']:
            if self.exists_player_object(player['id']):
                player_obj = self.get_player_object(player['id'])
            else:
                player_obj = self.Player(player['id'])
            if 'fullName' in player['player']:
                player_obj.set_name(player['player']['fullName'])
            if 'injuryStatus' in player['player']:
                player_obj.set_injuryStatus(player['player']['injuryStatus'])
            if 'status' in player:
                player_obj.set_status(player['status'])
            # noinspection SpellCheckingInspection
            if 'defaultPositionId' in player['player']:
                player_obj.set_primaryPosition(get_default_position(player['player']['defaultPositionId']))
            if 'eligibleSlots' in player['player']:
                eligible_slots = player['player']['eligibleSlots']
                position_list = []
                for position_id in eligible_slots:
                    if position_id < 16 and position_id != 12:
                        position_list.append(self.get_position(position_id))
                position_string = str(position_list)[1:-1]
                player_obj.set_eligiblePositions(position_string)
            if 'laterality' in player['player']:
                player_obj.set_throws(player['player']['laterality'])
            if 'stance' in player['player']:
                player_obj.set_bats(player['player']['stance'])

            # No longer available in this API call as of 2021
            # See set_next_start
            # if 'nextStartExternalId' in player['player']:
            # 	player_obj.set_start(player['player']['nextStartExternalId'])

            if 'proTeamId' in player['player']:
                player_obj.set_mlbTeam(self.team_name(player['player']['proTeamId']))
            if 'ownership' in player['player']:
                player_obj.set_auctionValueAverage(round(player['player']['ownership']['auctionValueAverage'], 2))
                player_obj.set_auctionValueAverageChange(
                    round(player['player']['ownership']['auctionValueAverageChange'], 2))
                player_obj.set_averageDraftPosition(round(player['player']['ownership']['auctionValueAverage'], 2))
                player_obj.set_percentOwned(round(player['player']['ownership']['percentOwned'], 2))
                player_obj.set_percentOwnedChange(round(player['player']['ownership']['percentChange'], 2))
                player_obj.set_percentStarted(round(player['player']['ownership']['percentStarted'], 2))

            if int(player_obj.percentOwned) >= 0:
                insert_list = list()
                insert_list.extend(player_obj.values())
                self.player_insert_list.append(insert_list)
                insert_many_list.append(tuple(insert_list))

            key = str(player_obj.get_espnid())
            player_status[key] = {}
            if 'injuryStatus' in player_status[key]:
                player_status[key]['injuryStatus'] = str(player_obj.get_injuryStatus())
            else:
                player_status[key]['injuryStatus'] = {}
                player_status[key]['injuryStatus'] = str(player_obj.get_injuryStatus())
            if 'status' in player_status[key]:
                player_status[key]['status'] = str(player_obj.get_status())
            else:
                player_status[key]['status'] = {}
                player_status[key]['status'] = str(player_obj.get_status())

            player_status[key]['nextStartID'] = str(player_obj.get_nextStartId())

            # No longer available in this API call as of 2021
            # See set_next_start
            # if 'nextStartID' in player_status[key]:
            # player_status[key]['nextStartID'] = str(player_obj.get_nextStartId())
            # else:
            # 	player_status[key]['nextStartID'] = {}
            # 	player_status[key]['nextStartID'] = str(player_obj.get_nextStartId())

            self.players[str(player['id'])] = player_obj

        self.current_player_status = player_status
        self.get_player_info_changes()
        return insert_many_list

    def get_player_data_json(self):
        # print_calling_function()
        leagueID = self.default_league
        url_name = "http://fantasy.espn.com/apis/v3/games/flb/seasons/" + self.year + \
                   "/segments/0/leagues/" + \
                   str(leagueID) + "?view=kona_playercard"
        headers = ['authority: fantasy.espn.com',
                   'accept: application/json',
                   'x-fantasy-source: kona',
                   'x-fantasy-filter: {"players":{"filterStatus":{"value":["FREEAGENT","WAIVERS","ONTEAM"]},'
                   '"filterSlotIds":{"value":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]}}}']
        # print("get_player_data_json: " + url_name)
        # self.logger_instance.debug(f'get_player_data_json: {url_name}')
        try:
            buffer = BytesIO()
            c = pycurl.Curl()
            c.setopt(c.URL, url_name)
            c.setopt(c.HTTPHEADER, headers)
            c.setopt(c.WRITEDATA, buffer)
            c.setopt(c.CONNECTTIMEOUT, self.TIMEOUT)
            c.setopt(c.CAINFO, certifi.where())
            c.perform()
            c.close()
            data = buffer.getvalue()
            self.player_data_json = json.loads(data)
        except Exception as ex:
            self.logger_exception(f'Exception in get_player_data_json')

    # json_formatted = json.dumps(self.player_data_json, indent=2)
    # print(json_formatted)

    def get_player_info_changes(self):
        # print_calling_function()
        now = datetime.now()  # current date and time
        date_time = now.strftime("%Y%m%d-%H%M%S")
        out_date = now.strftime("%Y%m%d")
        run_injury_updates = False
        for i in self.get_current_player_status():
            if i in self.get_db_player_status():
                for j in self.get_current_player_status()[i]:
                    if self.get_current_player_status()[i][j] != self.get_db_player_status()[i][j]:
                        current = self.get_current_player_status()[i][j]
                        db = self.get_db_player_status()[i][j]
                        espnid = i
                        name = str(self.get_player_object(i).name)
                        set_attr = j
                        old = self.get_db_player_status()[i][j]
                        new = self.get_current_player_status()[i][j]
                        where_attr = 'espnid'
                        if set_attr != "nextStartID":
                            self.push_msg_list.append(
                                tools.string_from_list([name, set_attr, 'old:', old, 'new:', new]))
                            run_injury_updates = True
                        self.DB.update_list("ESPNPlayerDataCurrent", set_attr, where_attr, (new, espnid))
                        self.DB.update_list("ESPNPlayerDataCurrent", "Date", where_attr, (out_date, espnid))
                        self.DB.update_list("ESPNPlayerDataCurrent", "UpdateTime", where_attr, (date_time, espnid))
                        self.DB.insert_list("ESPNStatusChanges", [out_date, date_time, espnid, set_attr, old, new])
            else:
                print("No corresponding data in ESPNPlayerDataCurrent for " + i)
                self.logger_instance.warning(f'No corresponding data in ESPNPlayerDataCurrent for {i}')
                # Insert into PlayerDataCurrent
                insert_many_list = list()
                insert_many_list.append(self.players[str(i)].get_player_data_fields())
                self.DB.insert_many("ESPNPlayerDataCurrent", insert_many_list)

        if run_injury_updates:
            self.run_injury_updates()

    @tools.try_wrap
    def send_push_msg_list(self):
        if len(self.push_msg_list):
            self.push_instance.push_list(self.push_msg_list, "Status changes")
            # self.push_instance.push_list_twtr(self.push_msg_list, "Status changes")
            self.push_msg_list.clear()
        return

    def run_query(self, query, msg=""):
        lol = []
        index = list()

        # print("Query: " + query)
        try:
            col_headers, rows = self.DB.select_w_cols(query)
            for row in rows:
                lol.append(row)
                index.append("")

            df = pd.DataFrame(lol, columns=col_headers, index=index)
            img = "mytable.png"
            dfi.export(df, img)
            self.push_instance.tweet_media(img, msg)
        except Exception as ex:
            print(str(ex))

        # df_styled = df.style.background_gradient()
        # adding a gradient based on values in cell
        return

    @tools.try_wrap
    def tweet_add_drops(self, dt=""):
        if dt == "":
            dt = self.date
        query = "select A.UpdateTime, PlayerName, A.TeamName,LegType," \
                " percentOwned, LeagueID from AddDrops A," \
                " ESPNPlayerDataCurrent E where A.ESPNID = E.ESPNID and" \
                " A.UpdateDate like '" + dt + "%' order by percentOwned desc"
        # print(query)
        self.run_query(query, "Adds / drops: ")
        return

    @tools.try_wrap
    def tweet_sprk_on_opponents(self):
        query = "select * from SPRKOnOpponents"
        # print(query)
        self.run_query(query, "SPRK on Opponents: ")
        return

    @tools.try_wrap
    def tweet_fran_on_opponents(self):
        query = "select * from FRANOnOpponents"
        # print(query)
        self.run_query(query, "FRAN on Opponents: ")
        return

    @tools.try_wrap
    def tweet_oppo_rosters(self):
        query = "select * from OppoRosters"
        # print(query)
        self.run_query(query, "Oppo Rosters ")
        return

    @tools.try_wrap
    def tweet_daily_schedule(self, dt="", msg=""):
        if dt == "":
            dt = self.date
        if msg == "":
            msg = "Daily schedule for " + str(dt)
        query = "select Date, G.GameID,A.AwayTeam,H.HomeTeam," \
                " substr(GameTime,9,17) as Time from ESPNGameData G, " \
                "(select G.GameID,T.MLBTeam as AwayTeam from ESPNGameData G," \
                " ESPNMLBTeams T where " \
                "( G.AwayTeamID = T.MLBTeamID ) and Date = " + dt + \
                " order by GameID) A, " \
                "(select G.GameID,T.MLBTeam as HomeTeam from ESPNGameData G," \
                " ESPNMLBTeams T where " \
                "( G.HomeTeamID = T.MLBTeamID ) and Date = " + dt + \
                " order by GameID) H where G.GameID = A.GameID " \
                "and G.GameID = H.GameID order by Time"

        print(query)
        self.run_query(query, msg)
        return

    @tools.try_wrap
    def refresh_espn_schedule(self):
        nextyear = str(int(self.year) + 1)
        fromdate = self.year + "0000"
        todate = nextyear + "0000"
        url_name = "https://fantasy.espn.com/apis/v3/games/flb/seasons/" + \
                   self.year + "?view=proTeamSchedules_wl"

        with urllib.request.urlopen(url_name, timeout=self.TIMEOUT) as url:
            data = json.loads(url.read().decode())
            # json_formatted = json.dumps(data, indent=2)
            # print(json_formatted)
            entries = list()
            gamedict = dict()
            for team in data['settings']['proTeams']:
                if 'proGamesByScoringPeriod' in team:
                    for games in team['proGamesByScoringPeriod']:
                        for game in team['proGamesByScoringPeriod'][games]:
                            away = game['awayProTeamId']
                            home = game['homeProTeamId']
                            game_time = int(game['date']) / 1000
                            game_date = time.strftime("%Y%m%d",
                                                      time.localtime(game_time))
                            game_time = time.strftime("%Y%m%d%H%M%S",
                                                      time.localtime(game_time))
                            game_id = game['id']
                            if gamedict.get(game_id):
                                continue
                            else:
                                entries.append([game_date, game_id,
                                                home, away, game_time])
                                gamedict[game_id] = 1

            # entrystr = inst.string_from_list(entries)
            delcmd = "delete from ESPNGameData where Date > " + fromdate + " and Date < " + todate
            self.DB.delete(delcmd)
            self.DB.insert_many("ESPNGameData", entries)

    @tools.try_wrap
    def refresh_statcast_schedule(self):
        url_name = "http://statsapi.mlb.com/api/v1/schedule?sportId=1,&date=" + \
                   self.statcast_date
        # print("url is: " + url_name)
        entries = []
        column_names = ['date', 'game']
        with urllib.request.urlopen(url_name, timeout=self.TIMEOUT) as url:
            data = json.loads(url.read().decode())
            for gamedate in data['dates']:
                for game in gamedate['games']:
                    # print(self.date + "," + str(game['gamePk']))
                    entry = [self.date, game['gamePk']]
                    entries.append(entry)

        df = pd.DataFrame(entries, columns=column_names)

        table_name = "StatcastGameData"
        delcmd = "delete from " + table_name + " where date = " + self.date
        self.logger_instance.debug(f'refresh_statcast_schedule: {delcmd}')
        self.DB.delete(delcmd)
        df.to_sql(table_name, self.DB.conn, if_exists='append', index=False)

    def run_injury_updates(self):
        query = "select name, mlbTeam, OldValue,NewValue,percentOwned," \
                "eligiblePositions,Time from InjuryStatusHistory where Time like '" + \
                self.date + "%' order by percentOwned desc"

        self.run_query(query, "Today's injuries:")

        time.sleep(1)

        query = "Select * From InjuryMovesToMake"
        self.run_query(query, "IL moves to make:")

    def set_espn_position_map(self):
        position = {}
        query = "select PositionID, Position from ESPNPositions"
        rows = self.DB.query(query)
        for row in rows:
            position[str(row['PositionID'])] = str(row['Position'])
        return position

    def get_espn_trans_ids(self):
        espn_trans_ids = {}
        query = "select distinct ESPNTransID from ESPNRosterChanges"
        rows = self.DB.query(query)
        for row in rows:
            espn_trans_ids[row['ESPNTransID']] = row['ESPNTransID']
        return espn_trans_ids

    def set_ID_team_map(self):
        teamName = dict()
        query = "select distinct LeagueID, TeamID, TeamName from ESPNTeamOwners"
        rows = self.DB.query(query)
        for row in rows:
            if str(row['LeagueID']) not in teamName:
                teamName[str(row['LeagueID'])] = {}
            teamName[str(row['LeagueID'])][str(row['TeamID'])] = str(row['TeamName'])
        return teamName

    def get_leagues(self):
        leagues = {}
        query = "select distinct LeagueID from ESPNLeagues"
        rows = self.DB.query(query)
        for row in rows:
            leagues[str(row['LeagueID'])] = str(row['LeagueID'])[0]
        return leagues

    def set_owner_team_map(self):
        teamName = {}
        query = "select OwnerID, TeamName from ESPNTeamOwners"
        rows = self.DB.query(query)
        for row in rows:
            teamName[str(row['OwnerID'])] = str(row['TeamName'])
        return teamName

    def build_transactions(self, leagueID):

        url_name = "http://fantasy.espn.com/apis/v3/games/flb/seasons/" + \
                   self.roster_year + \
                   "/segments/0/leagues/" + \
                   str(leagueID) + "?view=mTransactions2"
        print("build_transactions: " + url_name)
        add_drop_count = 0
        try:
            with urllib.request.urlopen(url_name, timeout=self.TIMEOUT) as url:
                json_object = json.loads(url.read().decode())
                push_list = list()
                # push_list_twtr = list()
                espnid_list = list()
                rr_msg = ""
                if 'transactions' in json_object:
                    for transaction in json_object['transactions']:
                        espn_transaction_id = transaction['id']
                        seconds = int(transaction['proposedDate']) / 1000.000
                        sub_seconds = str(round(seconds - int(seconds), 3))[1:]
                        update_date = time.strftime("%Y%m%d", time.localtime(seconds))
                        update_time = time.strftime("%Y%m%d-%H%M%S", time.localtime(seconds))
                        update_time_hhmmss = time.strftime("%H%M%S", time.localtime(seconds))
                        update_time += str(sub_seconds)
                        team_name = self.teamName[str(leagueID)][str(transaction['teamId'])]

                        item_count = 0
                        if 'items' in transaction:
                            for i in transaction['items']:

                                trans_obj = self.Transaction(espn_transaction_id)
                                trans_obj.set_leagueID(leagueID)
                                trans_obj.set_update_date(update_date)
                                trans_obj.set_update_time(update_time, update_time_hhmmss)
                                trans_obj.set_fantasy_team_name(team_name)
                                trans_obj.set_status(transaction['status'])
                                trans_obj.set_leg_type(i['type'])

                                item_list = list()
                                item_count += 1

                                transaction_id = espn_transaction_id + str(item_count)
                                trans_obj.set_transid(transaction_id)
                                item_list.append(transaction_id)

                                from_position = ""
                                if int(i['fromLineupSlotId']) >= 0:
                                    from_position = \
                                        self.position[str(i['fromLineupSlotId'])]
                                trans_obj.set_from_position(from_position)

                                from_team = ""
                                if i['fromTeamId'] > 0:
                                    from_team = \
                                        self.teamName[str(leagueID)][str(i['fromTeamId'])]
                                trans_obj.set_from_team(from_team)

                                player_name = ""
                                espnid = ""
                                if str(i['playerId']) in self.player:
                                    player_name = self.player[str(i['playerId'])]
                                    espnid = str(i['playerId'])
                                    item_list.append(player_name)
                                else:
                                    print("Missing playerID for " +
                                          str(i['playerId']) + "(" + str(i) + ")")
                                trans_obj.set_player_name(player_name)
                                trans_obj.set_espnid(espnid)

                                to_position = "B"
                                if int(i['toLineupSlotId']) >= 0:
                                    to_position = self.position[str(i['toLineupSlotId'])]
                                    item_list.append(to_position)
                                trans_obj.set_to_position(to_position)

                                to_team = ""
                                if 'toTeamId' in i and i['toTeamId'] > 0:
                                    to_team = self.teamName[str(leagueID)][str(i['toTeamId'])]
                                    item_list.append(to_team)
                                trans_obj.set_to_team(to_team)

                                item_list.append(i['type'])
                                trans_obj.set_type(transaction['type'])

                                index = str(trans_obj.get_update_time()) + \
                                        str(trans_obj.get_transid())

                                if (espn_transaction_id not in self.espn_trans_ids) or \
                                        (int(trans_obj.get_update_time_hhmmss()) >=
                                         int(self.get_roster_lock_time())):
                                    if transaction['status'] == 'EXECUTED':

                                        if espn_transaction_id not in self.espn_trans_ids:
                                            if i['type'] == "ADD" or i['type'] == "DROP":
                                                add_drop_count += 1

                                        self.transactions[index] = trans_obj
                                        fields = trans_obj.get_transaction_fields()

                                        if (espn_transaction_id not in self.espn_trans_ids) \
                                                and i['type'] != '':
                                            # print("Build transactions "
                                            #       "insert ESPNRosterChanges fields:")
                                            # print(fields)
                                            self.logger_instance.debug(f'build_transactions {fields}')

                                            try:
                                                self.DB.insert_list("ESPNRosterChanges", fields)
                                            except Exception as ex:
                                                self.logger_exception(
                                                    f'Error: build_transactions insert ESPNRosterChanges')

                                            team_list = ["Called Shots",
                                                         "Gotta B'leve",
                                                         "High And Tight",
                                                         "Great Bambi",
                                                         "The Terminators",
                                                         "When Franimals Attack",
                                                         "Spring Rakers",
                                                         "Bush Did 9/11",
                                                         "Flip Mode",
                                                         "Avengers: Age Of Beltran",
                                                         "wOBA Barons"]

                                            if (i['type'] == "ADD" or i['type'] == "DROP") and \
                                                    (team_name == "Great Bambi" or team_name == "Bush Did 9/11"):
                                                espnid_list.append(espnid)
                                                rr_msg += f'{player_name} {i["type"]}\n'

                                            if i['type'] == "LINEUP" and team_name \
                                                    not in team_list:
                                                # print("Skipping lineup change team"
                                                #       " not on watch list: " + team_name)
                                                self.logger_instance.debug(f'Skipping lineup change team'
                                                                           f'not on watch list: {team_name}')
                                            else:
                                                if trans_obj.get_type() != "FUTURE_ROSTER":
                                                    push_str = \
                                                        self.push_instance.string_from_list(
                                                            [update_time,
                                                             team_name,
                                                             "from:", from_position,
                                                             "to:",
                                                             to_position, from_team,
                                                             to_team,
                                                             player_name, str(i['type'] or "")])
                                                    print("Push String: " + push_str)
                                                    self.logger_instance.debug(f'PUSH: {push_str}')
                                                    push_list.append(push_str)

            if len(espnid_list) > 0:
                list_str = str(tuple(espnid_list))
                list_str = list_str.replace(",)", ")")
                query = f'select Player, Team, LeagueID,Position from ESPNRosters' \
                        f' where espnid in {list_str} order by Player, Team'
                print(query)
                try:
                    self.run_query(query, f'Relevant rosters: {rr_msg}')
                except Exception as ex:
                    self.push_instance.push("Error in relevant roster query", "Error: " + str(ex))

            if len(push_list) > 4:
                self.push_instance.push("Over 4 transactions", "Look for table tweet")

            if len(push_list) > 0:
                self.push_instance.push_list(push_list, "Transactions")
                push_list.clear()

        except Exception as ex:
            self.logger_exception(f'ERROR in build_transactions')

        return add_drop_count

    def process_transactions(self):
        updates = list()
        adds = list()
        drops = list()
        for i in sorted(self.transactions.keys()):
            transaction = self.transactions[i]
            trans_type = transaction.get_leg_type()
            trans_period = transaction.get_type()
            # player_name = transaction.get_player_name()
            # espn_trans_ids from RosterChanges table ESPNTransID
            # print("Transaction inside process_transactions:")
            # print(transaction.get_transaction_fields())
            if transaction.get_espntransid() not in self.espn_trans_ids or \
                    int(transaction.get_update_time_hhmmss()) >= \
                    int(self.get_roster_lock_time()):
                # print("New transaction " + trans_type)
                # print(transaction.get_transaction_fields())
                if trans_type == "LINEUP" and trans_period == "ROSTER":
                    updates.append(transaction)
            if transaction.get_espntransid() not in self.espn_trans_ids:
                if trans_type == "ADD":
                    adds.append(transaction)
                if trans_type == "DROP":
                    drops.append(transaction)
        # else:
        # print("Old transaction at " + str(transaction.get_update_time_hhmmss()))
        # print("process_transactions:")
        self.process_updates(updates)
        self.process_adds(adds)
        self.process_drops(drops)
        return

    def process_updates(self, updates):
        # print("Number of process_updates:")
        # takes a list of transaction objects
        # print(len(updates))
        for transaction in updates:
            # print("Update")
            to_position = transaction.get_to_position()
            leagueID = transaction.get_leagueID()
            # print(transaction.get_to_position())
            espnid = transaction.get_espnid()
            # print(transaction.get_espnid())
            # self.logger_instance.debug(f'process_updates: {}')
            self.DB.update_data("Update ESPNRosters set Position = ? "
                                "where ESPNID = ? and LeagueID = ?",
                                (to_position, espnid, leagueID))

    def process_adds(self, adds):
        # print("Number of process_adds:")
        # takes a list of transaction objects
        # print(len(adds))
        # entry = list()
        for transaction in adds:
            # print("Add")
            player_name = transaction.get_player_name()
            to_team = transaction.get_to_team()
            leagueID = transaction.get_leagueID()
            espnid = str(transaction.get_espnid())
            to_position = "B"
            update_time = transaction.get_update_time()
            # insert_many_list = list()
            entry = [player_name, to_team, leagueID,
                     espnid, to_position, update_time, self.year]
            # print("Build transactions Rosters insert command:")
            # print(entry)
            self.DB.insert_list("ESPNRosters", entry)

    def process_drops(self, drops):
        # print("Number of process_drops:")
        # print(len(drops))
        for transaction in drops:
            leagueID = transaction.get_leagueID()
            espnid = str(transaction.get_espnid())
            command = 'DELETE FROM ESPNRosters WHERE leagueID =? and ESPNID = ?'
            params = (leagueID, espnid,)
            self.DB.delete_item(command, params)

    def load_position_dict(self, use_pickle=True):
        position_dict = {}
        self.msg += "load_position_dict()\n\n"
        if use_pickle and path.exists("dict.pickle") and open("dict.pickle", "rb"):
            self.msg += "Load from pickle\n\n"
            pickle_in = open("dict.pickle", "rb")
            position_dict = pickle.load(pickle_in)
        else:
            self.msg += "Load from DB\n\n"
            c = self.DB.select("SELECT PositionID, Position from ESPNPositions")
            for t in c:
                position_dict[str(t[0])] = str(t[1])
        pickle_out = open("dict.pickle", "wb")
        pickle.dump(position_dict, pickle_out)
        pickle_out.close()
        return position_dict

    def populate_team_owners(self, leagueID):
        url_name = "http://fantasy.espn.com/apis/v3/games/flb/seasons/" + self.year + \
                   "/segments/0/leagues/" + \
                   str(leagueID)
        print("populate_team_owners: " + url_name)
        with urllib.request.urlopen(url_name, timeout=self.TIMEOUT) as url:
            json_object = json.loads(url.read().decode())

            # json_formatted = json.dumps(json_object, indent=2)
            for team in json_object['teams']:
                print("Team:")
                print(team)
                for owner in team['owners']:
                    insert_list = [owner, str(leagueID), str(team['id']),
                                   str(team['location']) + " " + team['nickname']]
                    self.DB.insert_list("ESPNTeamOwners", insert_list)

    @tools.try_wrap
    def run_transactions(self, teams=0):
        # print_calling_function()
        if not teams:
            teams = self.get_active_leagues()
        self.espn_trans_ids = self.get_espn_trans_ids()
        add_drop_count = 0
        for team in teams:
            add_drop_count += self.build_transactions(team)
            # print(f'Total add/drops: {add_drop_count}')
            time.sleep(1.5)
        if add_drop_count > 0:
            self.tweet_add_drops()
        if int(self.get_hhmmss()) > int(self.get_roster_lock_time()):
            # print("Process transactions:")
            time.sleep(.05)
            self.process_transactions()
            time.sleep(.05)
        else:
            print(f'Time: {self.get_hhmmss()} vs roster lock at {self.get_roster_lock_time()}')

    def set_espn_default_league(self):
        leagueID = 0
        query = "select LeagueID from ESPNDefaultLeague"
        rows = self.DB.query(query)
        for row in rows:
            leagueID = row['LeagueID']
        return leagueID
