__author__ = 'chance'

import sys
sys.path.append('./modules')

from bs4 import BeautifulSoup as bs
import push
import tools
import time

SEASON = 2023
sleep_interval = 10
# Selenium
driver = tools.get_driver("headless")
inst = push.Push()

def get_page(data):
	[name, home, league, team, week] = data
	url = f"https://fantasy.espn.com/football/boxscore?leagueId=" \
	      f"{str(league)}&matchupPeriodId={str(week)}&seasonId={SEASON}&teamId={str(team)}"
	print("url is: " + url)

	driver.get(url)
	time.sleep(sleep_interval)
	html = driver.page_source
	soup = bs(html, "html.parser")
	results = soup.find_all('div',{"class": ["statusLabel"]})
	away_score = str(results[2].text).split(':')[1]
	home_score = str(results[6].text).split(':')[1]
	margin = home * round((float(away_score) - float(home_score)),2)

	push_str = f'{name}: {margin} \n'
	print(push_str)
	#inst.push(push_str,"")
	return push_str


def main():
	table = [
		["FOMO", -1.00, 919257635, 8,13],
		["FRAN", -1.00, 1898955257, 1,13],
		["RULE", -1.00, 2103345024, 9,13]
	]
	push_str = ""
	while True:
		for lg in table:
			push_str += get_page(lg)
			time.sleep(2)
		inst.push(push_str, "")
		inst.tweet(push_str)
		print(push_str)
		push_str = ""
		time.sleep(120)
	driver.close()


if __name__ == "__main__":
	main()
