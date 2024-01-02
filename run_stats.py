__author__ = 'chance'

import sys
sys.path.append('./modules')
sys.path.append('..')

from player_stats import Stats
def main():
	process = Stats()
	process.start(threaded=True, sleep_interval=240)


if __name__ == "__main__":
	main()
