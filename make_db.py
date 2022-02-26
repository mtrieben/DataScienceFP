from cmath import nan
from tkinter import N
from bs4 import BeautifulSoup
import requests
import urllib
import sqlite3
import csv
import datetime


with open('MTA_recent_ridership.csv', newline='') as f:
    reader = csv.reader(f)
    mta_data = list(reader)


# print(mta_data[1])

cleaned_mta_data = []
for r in mta_data[1:]:
    row_info = []
    counter = 0
    add = True
    for c in r:
        if c == '#VALUE!' or c == '':
            add = False
            break
        if counter == 0:
            date = c.split('/')
            row_info.append(datetime.date(int(date[2]), int(date[0]), int(date[1])).isoformat())
        elif counter == 1 or counter == 7 or counter == 11:
            row_info.append(int(c))
        elif counter == 2 or counter == 8 or counter == 12:
            total = float(c[:-1])
            row_info.append(total)
        counter += 1
    if add:
        cleaned_mta_data.append(row_info)

print(len(cleaned_mta_data))

with open('covid_data.csv', newline='') as f1:
    covid_reader = csv.reader(f1)
    covid_data = list(covid_reader)

# print(covid_data[1])
cleaned_covid_data = {}
for r in covid_data[1:]:
    row_info = []
    counter = 0
    add = True
    for c in r:
        if c == '#VALUE!' or c == '':
            add = False
            break
        if counter == 0:
            date = c.split('/')
            row_info.append(datetime.date(int(date[2]), int(date[0]), int(date[1])).isoformat())
        elif counter == 1:
            row_info.append(int(c))
        counter += 1
    if add:
        cleaned_covid_data[row_info[0]] = row_info[1]

print(len(cleaned_covid_data))

# Loads bike data into output file

# with open('Jan2019_bike.csv', newline='') as f2:
#     bike_reader = csv.reader(f2)
#     bike_data = list(bike_reader)

# cleaned_bike_data = {}
# for r in bike_data[1:]:
#     counter = 0
#     add = True
#     if r[2] == '#VALUE!' or r[2] == '':
#         add = False
#         break
#     cleaned_date = r[2].split(' ')[0].split('-')
#     date_format = datetime.date(int(cleaned_date[0]), int(cleaned_date[1]), int(cleaned_date[2])).isoformat()
#     if date_format in cleaned_bike_data:
#         cleaned_bike_data[date_format] += 1
#     else:
#         cleaned_bike_data[date_format] = 1
    
# print(cleaned_bike_data)

# w = csv.writer(open("output.csv", "a"))

# for key, val in cleaned_bike_data.items():

#     # write every key and value to file
#     w.writerow([key, val])


with open('output.csv', newline='') as f2:
    bike_reader = csv.reader(f2)
    bike_data = dict(bike_reader)

print(len(bike_data))

conn = sqlite3.connect('data.db')
c = conn.cursor()

c.execute('DROP TABLE IF EXISTS "transport";')

c.execute('CREATE TABLE transport(full_date text not null, year int, month int, day int, covid_cases int, subway_ridership int, percent_subway_ridership float, mnr_ridership int, percent_mnr_ridership float, bridge_tunnel_usage int, percent_bridge_tunnel_usage float, citibike_usage int, percent_citibike_usage float, PRIMARY KEY(full_date))')
conn.commit()

for row in cleaned_mta_data:
    if row[0] in cleaned_covid_data and row[0] in bike_data:
        split_date = row[0].split('-')
        pre_pandemic_date = '2019-' + split_date[1] + '-' + split_date[2]
        if int(bike_data[row[0]]) > 5 and int(bike_data[pre_pandemic_date]) > 5:
            c.execute('INSERT INTO transport VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (row[0], 
                int(split_date[0]), int(split_date[1]), int(split_date[2]), int(cleaned_covid_data[row[0]]), 
                int(row[1]), float(row[2]), int(row[3]), float(row[4]), int(row[5]), float(row[6]), 
                int(bike_data[row[0]]), float((int(bike_data[row[0]]) / int(bike_data[pre_pandemic_date]))*100.0)))
            conn.commit()


