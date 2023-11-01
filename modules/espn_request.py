__author__ = 'chance'


import push
import json
import requests

SLEEP_INTERVAL = 10
ESPN_BASE = 'https://fantasy.espn.com/apis/v3/games/ffl/seasons/'
DEFAULT_YEAR = 2023
DEFAULT_URL = f"{ESPN_BASE}{str(DEFAULT_YEAR)}/segments/0/leaguedefaults/3?view=kona_player_info"

# https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leaguedefaults/1?view=kona_player_info
# https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/1221578721?&view=kona_player_info

DEFAULT_OUTPUT_FILE = "./data/ESPNdata.json"

# Selenium
# driver = tools.get_driver("headless")
inst = push.Push(calling_function="ESPNRequest")


class Request(object):

    def __init__(self):
        self._x = None
        self._BASE = ESPN_BASE
        self._DEFAULT_URL = DEFAULT_URL
        self._year = DEFAULT_YEAR
        self._default_filters = {
            "players": {"limit": 1500, "sortDraftRanks": {"sortPriority": 100, "sortAsc": True, "value": "PPR"}}}
        self._filters = {
            "players": {"limit": 1500, "sortDraftRanks": {"sortPriority": 100, "sortAsc": True, "value": "PPR"}}}
        self._headers = {'x-fantasy-filter': json.dumps(self._filters)}
        self._DEFAULT_OUTPUT_FILE = DEFAULT_OUTPUT_FILE
        inst.logger_instance.info(f"\nInitialized Request object with default url: {self._DEFAULT_URL}\n")

    @property
    def filters(self):
        return self._filters

    def DEFAULT_URL(self):
        print("Default url: " + str(self._DEFAULT_URL))
        return self._DEFAULT_URL

    def set_limit(self, value):
        self._filters["players"]["limit"] = value
        self._headers = {'x-fantasy-filter': json.dumps(self._filters)}
        # print("Limit changed to: " + str(value))
        # print(f"filters: {self._filters}")

    @property
    def year(self):
        return self._year

    @year.setter
    def year(self, value):
        self._year = value

    @year.getter
    def year(self):
        return self._year

    @property
    def x(self):
        return self._x

    @x.setter
    def x(self, value):
        # print("setter of x called")
        self._x = value

    @x.deleter
    def x(self):
        # print("deleter of x called")
        del self._x

    # {"players":{"filterStatus":{"value":["FREEAGENT","WAIVERS"]},"filterSlotIds":{"value":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,23,24]},"filterRanksForScoringPeriodIds":{"value":[14]},"sortPercOwned":{"sortPriority":2,"sortAsc":false},"sortDraftRanks":{"sortPriority":100,"sortAsc":true,"value":"STANDARD"},"limit":50,"filterRanksForSlotIds":{"value":[0,2,4,6,17,16]},"filterStatsForTopScoringPeriodIds":{"value":2,"additionalValue":["002021","102021","002020","11202114","022021"]}}}
    # {"players":{"filterSlotIds":{"value":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,23,24]},"filterRanksForScoringPeriodIds":{"value":[14]},"limit":50,"offset":0,"sortPercOwned":{"sortAsc":false,"sortPriority":1},"sortDraftRanks":{"sortPriority":100,"sortAsc":true,"value":"STANDARD"},"filterRanksForRankTypes":{"value":["PPR"]},"filterRanksForSlotIds":{"value":[0,2,4,6,17,16]},"filterStatsForTopScoringPeriodIds":{"value":2,"additionalValue":["002021","102021","002020","11202114","022021"]}}}

    # c = C()
    # c.x = 'foo'  # setter called
    # foo = c.x    # getter called
    # del c.x      # deleter called

    def make_request(self, write=False, print_flag=False, url=None, output_file=None, filters=None):

        url = url or self.DEFAULT_URL()
        output_file = output_file or self._DEFAULT_OUTPUT_FILE
        headers = self._headers
        if filters:
            self._filters = filters
            # print(f"Set filters: {self._filters}")
        else:
            self._filters = self._default_filters
        # print(f"Filters: {self._filters}")
        self._headers = {'x-fantasy-filter': json.dumps(self._filters)}
        if print_flag:
            print(f"headers: {self._headers}")
            print(f"make_request, using url: {url}")
        # url = 'http://fantasy.espn.com/apis/v3/games/flb/seasons/2021/segments/0/leaguedefaults/1?view=kona_player_info'
        # url = 'https://fantasy.espn.com/apis/v3/games/ffl/seasons/' + str(year) + '/segments/0/leaguedefaults/1?view=kona_player_info'
        # filters = {"players": {"limit": 500, "sortDraftRanks": {"sortPriority": 100, "sortAsc": True, "value": "STANDARD"}}}
        # headers = {'x-fantasy-filter': json.dumps(filters)}

        resp = None
        try:
            resp = requests.get(url, headers=headers).json()

            if write:
                with open(output_file, 'w') as outfile:
                    json.dump(resp, outfile)
                    inst.logger_instance.info("Printed result to " + output_file)

            if print_flag:
                print(f"Resp: {resp}")

        except Exception as ex:
            inst.logger_instance.info(f"Error in make_request(): {ex}")

        return resp
