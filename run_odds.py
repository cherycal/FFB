__author__ = 'chance'

import csv
import datetime
import sys
sys.path.append('./modules')
sys.path.append('..')

from player_stats import Stats
import sqldb
import espn_request
import pandas as pd

fdb = sqldb.DB('Football.db')
request_instance = espn_request.Request()

def game_odds(game):
	file_name = './data/odds.csv'
	table_name = "Odds"
	url = str(f"https://sports.core.api.espn.com/v2/sports/football/"
	          f"leagues/nfl/events/{game}/competitions/{game}/odds")
	odds = request_instance.make_request(url=url, output_file=f"./data/ESPNOdds.json", write=True)
	update_time = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")
	with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
		# creating a csv writer object
		csv_writer = csv.writer(csv_file)
		csv_writer.writerow(['gameid', 'line', 'spread', 'OU', 'favorite',
		                     'provider','update_time'])
		for odds_quote in odds.get('items'):
			provider = None
			if odds_quote.get('provider'):
				provider = odds_quote.get('provider').get('name',"?")
			favorite = "?"
			if odds_quote.get('awayTeamOdds'):
				if odds_quote.get('awayTeamOdds').get('favorite') is True:
					favorite = "Away"
				else:
					favorite = "Home"
			if favorite != "?":
				print(f"{game},{odds_quote.get('details')},{odds_quote.get('spread')},{odds_quote.get('overUnder')},"
				      f"{favorite},{provider},{update_time}")
				csv_writer.writerow([game,odds_quote.get('details'),odds_quote.get('spread'),
				                     odds_quote.get('overUnder'),favorite,provider,update_time])
	df = pd.read_csv(file_name)

	# print(df)
	# delcmd = f"delete from {table_name} where league = '{data['league_name']}'"
	# # print(delcmd)
	# try:
	# 	self.DB.delete(delcmd)
	# except Exception as ex:
	# 	print(f"Exception in {delcmd}: {ex}")
	# 	self.DB.reset()

	try:
		df.to_sql(table_name, fdb.conn, if_exists='append', index=False)
	except Exception as ex:
		print(f"Exception in df.to_sql: {ex}")
		fdb.reset()

def odds():
	games_query = fdb.query(f"select distinct game_id from LeagueSchedule "
	                        f"where game_week = (select max(week) from PlayerStats where "
	                        f"Year = ( select max(year) from CurrentSeason)) and year = "
	                        f"( select max(year) from CurrentSeason)")

	[game_odds(row['game_id']) for row in games_query]


def main():
	#Stats().start(threaded=False, sleep_interval=240)
	odds()


if __name__ == "__main__":
	main()
