__author__ = 'chance'

import sys
import pandas as pd
import csv

sys.path.append('./modules')
import espn_request
import sqldb

fdb = sqldb.DB('Football.db')


def get_player_stats(year):
    data = dict()
    data['player_stats'] = dict()
    ###############
    # LIMIT = 1500
    ##############
    position_names = {
        '0': 'QB',
        '1': 'QB',
        '2': 'RB',
        '3': 'WR',
        '4': 'TE',
        '5': 'TE',
        '16': 'D',
        '17': 'K',
        '20': 'B',
        '21': 'IR',
        '23': '23'
    }
    ##############
    player_stats = {}
    request_instance = espn_request.Request()
    request_instance.set_limit(1500)
    url = f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/{year}/segments/0/leaguedefaults/3?view=kona_player_info"
    if year <= 2017:
        print(f"Year must be after 2018")
        exit(0)

    player_data = request_instance.make_request(url=url,
                                                output_file=f"./data/ESPNPlayerStats.json",
                                                write=True)
    players = player_data['players']
    for player in players:
        # print(player['player']['fullName'])
        player_name = player['player']['fullName']
        player_id = str(player['player']['id'])
        player_team = player['player']['proTeamId']
        player_status = player['player'].get('injuryStatus', "")
        player_positions = player['player'].get('eligibleSlots', [''])
        player_position = position_names.get(str(player_positions[0]), '')
        if player_position == '':
            player_position = position_names.get(str(player_positions[1]), '')
        # roster_status = player.get('status', "")
        ownership = player['player'].get('ownership', False)
        percentChange = ""
        percentOwned = ""
        percentStarted = ""
        if ownership:
            percentChange = player['player']['ownership']['percentChange']
            percentOwned = player['player']['ownership']['percentOwned']
            percentStarted = player['player']['ownership']['percentStarted']
        if player_stats.get(player_id) is None:
            player_stats[player_id] = {}
            player_stats[player_id]['info'] = {}
            player_stats[player_id]['stats'] = {}
            player_stats[player_id]['info']['id'] = player_id
            player_stats[player_id]['info']['name'] = player_name
            player_stats[player_id]['info']['proTeam'] = player_team
            player_stats[player_id]['info']['injuryStatus'] = player_status
            # player_stats[player_id]['info']['rosterStatus'] = roster_status
            player_stats[player_id]['info']['percentChange'] = percentChange
            player_stats[player_id]['info']['percentOwned'] = percentOwned
            player_stats[player_id]['info']['percentStarted'] = percentStarted
            player_stats[player_id]['info']['position'] = player_position
            # player_stats[player_id]['proj'] = [0 for i in range(WEEKS)]
            # player_stats[player_id]['act'] = [0 for i in range(WEEKS)]
            player_stats[player_id]['stats']['proj'] = {}
            player_stats[player_id]['stats']['act'] = {}
        stats = player['player'].get('stats', [])
        for stat in stats:
            if stat['statSplitTypeId'] == 1 and stat['seasonId'] == year:
                week = str(stat['scoringPeriodId'])
                total = round(stat['appliedTotal'], 2)
                if stat['statSourceId'] == 0:
                    player_stats[player_id]['stats']['act'][week] = total
                else:
                    player_stats[player_id]['stats']['proj'][week] = total

    data['player_stats'] = player_stats
    return data


def write_player_stats(data, year):
    file_name = "./data/player_stats_file.csv"
    table_name = "PlayerStats"
    leagueId = 0
    # is_header = True
    with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
        # creating a csv writer object
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['id', 'name', 'week', 'proj', 'act', 'leagueId', 'year'])
        for player in data['player_stats']:
            player_id = data['player_stats'][player]['info']['id']
            name = data['player_stats'][player]['info']['name']
            stats = data['player_stats'][player]['stats']
            for week in stats['proj']:
                proj = stats['proj'].get(week, "")
                act = stats['act'].get(week, "")
                # print([player_id, name, week, proj, act, leagueId, data['year']])
                csv_writer.writerow([player_id, name, week, proj, act, leagueId, year])
    df = pd.read_csv(file_name)
    # print(df)

    if df.size > 0:
        delcmd = f"delete from {table_name} where Year = {year}"
        fdb.delete(delcmd)
        df.to_sql(table_name, fdb.conn, if_exists='append', index=False)

    return 0


def main():
    year = 2023
    player_stats = get_player_stats(year)
    write_player_stats(player_stats, year)


if __name__ == "__main__":
    main()
