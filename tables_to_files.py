import sys

sys.path.append('./modules')
import sqldb
import tools

mode = "PROD"

if mode == "TEST":
    bdb = sqldb.DB('FootballTest.db')
else:
    bdb = sqldb.DB('Football.db')

def run_tables():
    tables = ['PlayerRosters']
    for tbl in tables:
        bdb.table_to_csv(tbl)

@tools.try_wrap
def run_all_tables():
    tables = ['PlayerRosters']
    for tbl in tables:
        bdb.table_to_csv(tbl)

@tools.try_wrap
def main():
    run_all_tables()


if __name__ == "__main__":
    main()
