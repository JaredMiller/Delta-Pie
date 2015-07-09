import MySQLdb
import csv
import os
import datetime
from numbers import Number
from decimal import *
from collections import deque
import itertools
from itertools import chain
import csv

# if we are in debug mode, make sure we are verbose with ssh logging too
env = {}
env["dbuser"] = "<user>"
env["dbpass"] = "<pass>"
env["dbhost"] = "<host>"


def db_connect():
	return MySQLdb.connect(host=env["dbhost"], port=3306, user=env["dbuser"], passwd=env["dbpass"])

def calc_per_change(a,b):
	getcontext().prec = 2
	return round(((((b-a)/Decimal(a))*100) if (isinstance(a, Number) and isinstance(b,Number) and a > 0) else 0),2)

def get_modded_rows(sql,degrees):
	
	#db work to get data
	db = db_connect()
	c = db.cursor()
	c.execute(sql)

	#get column names
	col_names = [i[0] for i in c.description]

	#setup the list of queues to hold tracked data
	degreeQueues = []
	for degree in degrees:
		degreeQueues.append(deque([],degree))

	#first get the full dataset
	currentRow = c.fetchone()


	headerDone = False
	with open('outFile.csv', 'w') as csvfile:
		outcsv = csv.writer(csvfile, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
		while currentRow is not None:
			#get only numbers in the current row
			merged = zip(col_names, currentRow)

			#only get the numbers, can't divide strings!  Also make parallel lists of labels and values
			currentRow = [x for x in merged if isinstance(x[1],Number)]
			filteredRow =  map(lambda x : x[1], currentRow)
			filteredNames = map(lambda x : x[0], currentRow)


			#now, for each degree we want to calc
			for q in degreeQueues:
				#first check if we have enough data to do this calc for this degree, we know that once the deque is full we can start processing 
				if q.maxlen == len(q):  

					#This is a hack, need to reorder this code to allow header generation earlier
					if headerDone == False:
						outcsv.writerow(list(chain(*zip(filteredNames,map(lambda x : "{}_{}".format(x,len(q)),filteredNames)))))
						headerDone = True
					
					#loop through each entry and add to the output file
					#so this code does
					# 1. Extracts all previous rows in this deque horizontally - (zip(*q)) 
					# 2. and then sums them vertically - map(sum, zip(*q))
					# 3. then extracts the sum next to the current row - zip(map(sum, zip(*q)), filteredRow)
					# 4. next to getting the number we are on for refernce - enumerate(
					# 5. It then loops through each one where we will calculate the change
					r = list(chain(*zip(filteredRow,[calc_per_change(a/len(q),b) for a,b in zip(map(sum, zip(*q)), filteredRow)])))
					outcsv.writerow(r)

					#This is for debugging only	
					#for i, (a,b) in enumerate(zip(map(sum, zip(*q)), filteredRow)):
						#print i, "sumed: ", a, "averaged: ", a/len(q), "current row:",  b, "Percent change: ", calc_per_change(a/len(q),b), "column name: ", filteredNames[i], "degree: ", len(q)

				#add the row we just processed to the historical list for processing on next iteration		
				q.append(filteredRow)
			#get the next row
			currentRow = c.fetchone()
		#cleanup time
	c.close()
	db.close()


def process(sql, degreeNums):
	degs = []
	for deg in degreeNums:
		try: 
			degs.append(int(deg))
		except:
			print "Gotta give me all ints for the degrees friend, that other stuff.. not gonna cut it"
			return

	get_modded_rows(sql, degs)


process('select * from atmosphere.ops_weekly_health_report order by start_of_week asc limit 10000', ["3"])


