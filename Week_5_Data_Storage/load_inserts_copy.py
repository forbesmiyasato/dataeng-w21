# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import io
import csv
import time
import psycopg2
import argparse
import re
import csv

DBname = "data_storage"
DBuser = "forbes"
DBpwd = "forbes"
TableName = 'CensusData'
Datafile = ""  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015


def initialize():
  global Year

  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  parser.add_argument("-y", "--year", default=Year)
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable
  Year = args.year

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = True
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
        	DROP TABLE IF EXISTS {TableName};
        	CREATE TABLE {TableName} (
            	Year                INTEGER,
              CensusTract         NUMERIC,
            	State               TEXT,
            	County              TEXT,
            	TotalPop            INTEGER,
            	Men                 INTEGER,
            	Women               INTEGER,
            	Hispanic            DECIMAL,
            	White               DECIMAL,
            	Black               DECIMAL,
            	Native              DECIMAL,
            	Asian               DECIMAL,
            	Pacific             DECIMAL,
            	Citizen             DECIMAL,
            	Income              DECIMAL,
            	IncomeErr           DECIMAL,
            	IncomePerCap        DECIMAL,
            	IncomePerCapErr     DECIMAL,
            	Poverty             DECIMAL,
            	ChildPoverty        DECIMAL,
            	Professional        DECIMAL,
            	Service             DECIMAL,
            	Office              DECIMAL,
            	Construction        DECIMAL,
            	Production          DECIMAL,
            	Drive               DECIMAL,
            	Carpool             DECIMAL,
            	Transit             DECIMAL,
            	Walk                DECIMAL,
            	OtherTransp         DECIMAL,
            	WorkAtHome          DECIMAL,
            	MeanCommute         DECIMAL,
            	Employed            INTEGER,
            	PrivateWork         DECIMAL,
            	PublicWork          DECIMAL,
            	SelfEmployed        DECIMAL,
            	FamilyWork          DECIMAL,
            	Unemployment        DECIMAL
         	);	
         	ALTER TABLE {TableName} ADD PRIMARY KEY (Year, CensusTract);
         	CREATE INDEX idx_{TableName}_State ON {TableName}(State);
    	""")

		print(f"Created {TableName}")

def load(conn):

	with open(Datafile) as csvFile, conn.cursor() as cursor:
		csvFile.seek(0)
		next(csvFile)
		CSVdata = csv.reader(csvFile, delimiter=',')
		length = len(list(CSVdata))
		print(f"Loading {length} rows")
		start = time.perf_counter()
		cursor.copy_from(csvFile, TableName, sep=',')

		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
		cursor.execute(f"SELECT count(*) FROM {TableName};")
		print(cursor.fetchall())
		cursor.execute(f"SELECT count(*) FROM CensusData2;")
		print(cursor.fetchall())

def main():
    initialize()
    conn = dbconnect()

    if CreateDB:
    	createTable(conn)

    load(conn)


if __name__ == "__main__":
    main()



