#BeautifulSoup Station NOAA
import csv
import re
import requests
import time
import numpy as np
import pandas as pd
import mysql.connector as mysql
from bs4 import BeautifulSoup

#membuat table
# mycursor = db.cursor()
# mycursor.execute("select database();")
# record = mycursor.fetchone()
# print("You're connected to database: ", record)

# mycursor.execute('DROP TABLE IF EXISTS station_56003;')
# print('Creating table....')
# mycursor.execute("CREATE TABLE station_56003 (`Year` INT(4) NOT NULL, `Month` INT(2) NOT NULL, `Day` INT(2) NOT NULL, `Hour` INT(2) NOT NULL, `Minute` INT(2) NOT NULL, `Second` INT(2) NOT NULL, `T` INT(2) NOT NULL, `Height` Float(7,3) NOT NULL, `Date` DATETIME NOT NULL UNIQUE KEY)")
# print("56003 table is created....")


#connect MySQL
db = mysql.connect(
    host = "localhost",
    user = "root",
    passwd = "",
    database = "station"
)

# scrape datas
headers = ["Year", "Month", "Day", "Hour", "Minute", "Second", "T", "Height"]
page = requests.get('https://www.ndbc.noaa.gov/station_page.php?station=56003')
soup = BeautifulSoup(page.text, 'html.parser')
datas = []
dt = soup.find_all('textarea')[0].text
datas = dt.split('\n')[2:-1]

# membaca scrape to to array dan membaca data ke6
arr = []
arr = np.array([datas])


def listToString(s):
    str1 = ""
    for ele in s:
        str1 += ele
    return str1


coba = []
for item_list in arr:
    item_string = listToString(item_list)
    coba.append(item_string.split()[6])


# -----------------------------------------------

# menambahkan date dengan gabungan year, month,dll
def addDate():
    fixed_df = pd.read_csv('56003.csv', sep=',')  # , parse_dates=['Day'], dayfirst=True, index_col='Day'
    fixed_df['Date'] = pd.to_datetime(fixed_df[['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']])
    fixed_df.to_csv('56003-1.csv', index=False)
    return


# -----------------------------------------------

# menambahkan date dengan gabungan year, month,dll
def insertSQL():
    with open('56003-1.csv') as csv_file:
        csvfile = csv.reader(csv_file)
        header = next(csvfile)  # This skips the first row of the CSV file.
        all_value = []
        for row in csvfile:
            value = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
            all_value.append(value)

        # query insert database IGNORE untuk mengabaikan field yang sudah ada
        query = "INSERT IGNORE INTO station.station_56003 (Year,Month,Day,Hour,Minute,Second,T,Height,Date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

        mycursor = db.cursor()
        mycursor.executemany(query, all_value)
        db.commit()
        mycursor.close()
    return


# -----------------------------------------------


# perulangan interval data T
print(coba[0] == '1')
if coba[0] == '1':
    while True:
        print('Data 1')
        page = requests.get('https://www.ndbc.noaa.gov/station_page.php?station=56003')
        soup = BeautifulSoup(page.text, 'html.parser')
        datas1 = []
        dt = soup.find_all('textarea')[0].text
        datas1 = dt.split('\n')[2:-1]

        with open("56003.csv", "w") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(headers)

            for line in soup.select_one("#data").text.split("\n"):
                if re.fullmatch(r"[\d. ]{30}", line) and len(line.split()) == len(headers):
                    writer.writerow(line.split())

        addDate()
        insertSQL()
        time.sleep(21600)

elif coba[0] == '2':
    while True:
        print('Data 2')
        page = requests.get('https://www.ndbc.noaa.gov/station_page.php?station=56003')
        soup = BeautifulSoup(page.text, 'html.parser')
        datas1 = []
        dt = soup.find_all('textarea')[0].text
        datas1 = dt.split('\n')[2:-1]

        with open("56003.csv", "w") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(headers)

            for line in soup.select_one("#data").text.split("\n"):
                if re.fullmatch(r"[\d. ]{30}", line) and len(line.split()) == len(headers):
                    writer.writerow(line.split())

        addDate()
        insertSQL()
        time.sleep(3600)


else:
    while True:
        print('Data 3')
        page = requests.get('https://www.ndbc.noaa.gov/station_page.php?station=56003')
        soup = BeautifulSoup(page.text, 'html.parser')
        datas1 = []
        dt = soup.find_all('textarea')[0].text
        datas1 = dt.split('\n')[2:-1]

        with open("56003.csv", "w") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(headers)

            for line in soup.select_one("#data").text.split("\n"):
                if re.fullmatch(r"[\d. ]{30}", line) and len(line.split()) == len(headers):
                    writer.writerow(line.split())

        addDate()
        insertSQL()
        time.sleep(600)