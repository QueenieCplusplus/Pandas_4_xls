"""
2: Given a CSV listing homes sold, the sale date, and the ZIP code of the home, write a script to print out how many homes were sold each day.
3: Building on question 2, expand that script to break down the homes sold per day by ZIP code. Your output should look like:
Date,76325,789123,54375 07/13/2018,4,0,2 08/04/2017,6,9,0
"""

import csv


with open('sales_data.csv', 'rb') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
		row = map(int,(row['Zip1'],row['Zip2'],row['Zip3'],row['Zip4'],row['Zip5']))
		s = sum(row)
		print s





