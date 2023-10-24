import os
import sys
sys.path.append('./modules')
import sqldb, tools
import os.path
from os import path

fdb = sqldb.DB('Football.db')

print(fdb.query("Select * from ESPNRosters"))

