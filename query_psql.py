import time
import psycopg2.extras
import argparse
import re
import csv
import sys
import pandas as pd
import numpy as np

DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgres"
TableName1 = 'BreadCrumb'
TableName2 = 'Trip'

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
    
def load(conn, csvfile, table):

	with conn.cursor() as cursor:
		start = time.perf_counter()
		cursor.copy_from(csvfile, table, sep=",", null='None')
		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

def query4(conn):
    """ The longest (as measured by time) trip in your entire data set.
    Indicate the date, route #, and trip ID of the trip along with a visualization showing the entire trip.
    """
    cur = conn.cursor()
    cur.execute("select age(max(tstamp), min(tstamp)) as time, trip_id from breadcrumb group by trip_id, date(tstamp) order by time desc limit 1")
    rows = cur.fetchall()
    print("The row count: ", cur.rowcount)
    for row in rows:
        print(row)
    cur.close()    
    
def main():
	conn = dbconnect()
    query4(conn)
    
if __name__ == "__main__":
    main()