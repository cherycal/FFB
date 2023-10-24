import os
import sys
sys.path.append('./modules')
import sqldb, tools
import os.path
from os import path

bdb = sqldb.DB('Baseball.db')

# rows = bdb.query(cmd="select M.*, P.InjuryStatus as OldInjuryStatus from MostRecentPlayerData M, PlayerData P "
#                    "where M.Date = P.Date and M.ESPNID = P.ESPNID")
#
# for row in rows:
#     print(str(row['Name']) + ', ' + str(row['ESPNID']) + ', ' +
#           str(row['InjuryStatus']) + ', ' + str(row['OldInjuryStatus']) )
