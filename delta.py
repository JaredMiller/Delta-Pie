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

env = {}
env["dbuser"] = "user"
env["dbpass"] = "pass"
env["dbhost"] = "host"


def db_connect():
	return MySQLdb.connect(host=env["dbhost"], port=3306, user=env["dbuser"], passwd=env["dbpass"])

def calc_per_change(a,b):
	getcontext().prec = 2
	return round(((((b-a)/Decimal(a))*100) if (isinstance(a, Number) and isinstance(b,Number) and a > 0) else 0),2)

def get_modded_rows(sql,columns_of_record,degrees):
	
	#db work to get data
	db = db_connect()
	c = db.cursor()
	c.execute(sql)

	#get column names
	col_names = [i[0] for i in c.description]

	#if the the columns that are explicited requested to be included are not, throw an error
	if not set(columns_of_record) < set(col_names):
		print "Error: column of record not present in result set"
		return

	#setup the list of queues to hold tracked data
	degreeQueues = []
	for degree in degrees:
		degreeQueues.append(deque([],degree))

	#first get the full dataset and generate the header row
	currentRow = c.fetchone()
	m = zip(col_names, currentRow)
	cr = [x for x in m if isinstance(x[1],Number)]
	columns_of_record_names = [x[0] for x in m if x[0] in columns_of_record]

	#merge the columns of record, the degree and the actual column names
	final_names = columns_of_record_names + ['degree'] + (map(lambda x : x[0], cr))
	
	with open('outFile.csv', 'w') as csvfile:
		outcsv = csv.writer(csvfile, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
		#write header row
		outcsv.writerow(final_names)

		while currentRow is not None:
			#get only numbers in the current row
			merged = zip(col_names, currentRow)

			# Get the preserved values
			columns_of_record_values = [x[1] for x in merged if x[0] in columns_of_record]


			#only get the numbers, can't divide strings!  Also make parallel lists of labels and values
			currentRow = [x for x in merged if isinstance(x[1],Number)]
			filteredRow =  map(lambda x : x[1], currentRow)

			outcsv.writerow(columns_of_record_values + [0] + filteredRow)
			#now, for each degree we want to calc
			for q in degreeQueues:
				#first check if we have enough data to do this calc for this degree, we know that once the deque is full we can start processing 
				if q.maxlen == len(q):  

					
					#loop through each entry and add to the output file
					#so this code does
					# 1. Extracts all previous rows in this deque horizontally - (zip(*q)) 
					# 2. and then sums them vertically - map(sum, zip(*q))
					# 3. then extracts the sum next to the current row - zip(map(sum, zip(*q)), filteredRow)
					# 4. next to getting the number we are on for refernce - enumerate(
					# 5. It then loops through each one where we will calculate the change
					#r = list(chain(*zip(filteredRow,[calc_per_change(a/len(q),b) for a,b in zip(map(sum, zip(*q)), filteredRow)])))
					r = columns_of_record_values + [len(q)] + [calc_per_change(a/len(q),b) for a,b in zip(map(sum, zip(*q)), filteredRow)]
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


def process(sql, columns_of_record, degreeNums):
	degs = []
	for deg in degreeNums:
		try: 
			degs.append(int(deg))
		except:
			print "Gotta give me all numbers for the degrees friend, that other stuff.. not gonna cut it"
			return

	get_modded_rows(sql, columns_of_record, degs)

#sample data below
process("query", ['week'], ["2", "5", "10"])


