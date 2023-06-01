from time import sleep
from datetime import datetime, timedelta

import pandas as pd
import json
import os
import sys
import pymysql
from pymysql import Error

'''
for remote access - add HOSTNAME=localhost to env  
ssh -L 8676:127.0.0.1:3306 group08@cse191.ucsd.edu
'''
class dbClass:

    def __init__(self, type="JSON"):
        self.outType = type
        # AMAZON CLOUD - AWS DB Cluster
        print("connect to main DB")
        self.servername = "127.0.0.1"
        self.username = "root"
        self.password = "iotiot"
        self.dbname = "cse191"
        self.port = 8676
              
        if os.getenv('HOSTNAME') == "localpc":
            print("connect local ssh tunnel")
            self.port = 8676

        self.reconnect()

    def check_conn(self):

        # test connection
        try:
            if self.db.cursor().execute("SELECT now()") == 0:
                return self.reconnect()
            else:
                print("DB connection OK\n")
                return True
        except:
            print("Unexpected exception occurred: ", sys.exc_info())
            return self.reconnect()

    def reconnect(self):

        # try to connect 5 times
        retry = 5
        while retry > 0:
            try:
                print("connecting to DB...")
                self.db = pymysql.connect(
                    host=self.servername,
                    user=self.username,
                    password=self.password,
                    database=self.dbname,
                    port=self.port
                )
                retry = 0
                return True
            except:
                print("Unexpected exception occurred: ", sys.exc_info())
                retry -= 1
                if retry > 0:
                    print("retry\n")
                    sleep(2)
                else:
                    exit(-1)

        print("Success\n")

    def loadStudents(self, gn):
        if self.check_conn():
            stu_df = pd.DataFrame
            if gn == None:
                sqlStr = "SELECT * FROM cse191.students ORDER BY groupnumber"
            else:
                sqlStr = "SELECT * FROM cse191.students WHERE groupnumber=" + str(gn)
            print(sqlStr)
            cursor = self.db.cursor()
            result = None
            try:
                cursor.execute(sqlStr)
                result = cursor.fetchall()
                print(result)
                stu_df = pd.DataFrame.from_dict(result) 
                stu_df.columns=["id","name","email","groupnumber","groupname"]
                print(stu_df)
            except Error as e:
                print(f"The error '{e}' occurred")

            return stu_df
        
    def loadDevices(self, gn):
        if self.check_conn():
            dev_df = pd.DataFrame
            if gn == None:
                sqlStr = "SELECT * FROM cse191.devices"
            else:
                sqlStr = "SELECT * FROM cse191.devices WHERE groupnumber=" + str(gn)
            print(sqlStr)
            cursor = self.db.cursor()
            result = None
            try:
                cursor.execute(sqlStr)
                result = cursor.fetchall()
                print(result)
                dev_df = pd.DataFrame.from_dict(result) 
                dev_df.columns=["device_id","mac","lastseen_ts","last_rssi","groupname","location","lang","long","color","groupnumber","status"]
                print(dev_df)
            except Error as e:
                print(f"The error '{e}' occurred")

            return dev_df
        
    def logDevices(self, data):
        if self.check_conn():
            try:
                cursor = self.db.cursor()
                # extract info out of data
                gn = data.gn
                espmac = data.espmac

                # add all the devices seen by the ESP to ble_logs (info stored in a list of dicts)
                for i in range(len(data.devices)):
                    now = datetime.datetime.now()
                    sql_datetime = now.strftime('%Y-%m-%d %H:%M:%S')
                    device_string = "INSERT INTO cse191.ble_logs (device_mac, ble_mac, ble_rssi, groupname, groupnumber, log_ts, ble_count) VALUES \
                        (\"{}\",\"{}\",{}, \"{}\", {}, \"{}\", {})".format(espmac, data.devices[i].get("mac"), data.devices[i].get("rssi"), \
                        "The Sensor Squad", gn, sql_datetime, len(data.devices))
                    cursor.execute(device_string)
                    
                self.db.commit()
                return True
            except Error as e:
                print(f"The error '{e}' occurred")
        
        return False
    
    def logESPDevice(self, esp):
        if self.check_conn():
            try:
                cursor = self.db.cursor()
                group_id = esp.group_id
                espmac = esp.mac
                # check for the existence of this ESP
                cursor.execute("SELECT COUNT(*) FROM cse191.devices WHERE mac = '{}'".format(espmac))
                count = cursor.fetchall()[0][0]
                if (count == 0):
                    # insert this esp device into table
                    cursor.execute("INSERT INTO cse191.devices (mac, groupname, groupnumber) VALUES ('{}', '{}', {})".format(espmac, group_id, 8))
                
                self.db.commit()
                return True
            except Error as e:
                print(f"The error '{e}' occurred")
        
        return False
    
    def sendToDF(self, newRow):
        if self.check_conn():
            try:
                cursor = self.db.cursor()
                insertion_command_string = ', '.join('?' * len(newRow))
                print(newRow)
                # print(insertion_command_string)
                # query_string = 'INSERT INTO cse191.analytics_rimac_data VALUES (%s);' % insertion_command_string
                # print(query_string)
                cursor.execute("INSERT INTO cse191.analytics_rimac_data VALUES(?, ?, ?, ?)", newRow)
                #cursor.execute(query_string, newRow)
            except Error as e: 
                print(f"The error '{e}' occurred")
        return 
    
    def getDF(self, time):
        if self.check_conn():
            start_time = time
                    
            # Convert the string to a datetime object
            datetime_object = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")

            # Add 20 seconds to the datetime object
            new_datetime = datetime_object + timedelta(seconds=20)

            # Convert the new datetime back to a string
            end_time = new_datetime.strftime("%Y-%m-%d %H:%M:%S")

#df.columns=['DEVICE MAC','A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', 'O', 'P', 'Q', 'R', 'T', 'U', 'V', 'W', 'X', "TIME"]
            hashmap = {
    "CC:50:E3:A8:F3:00": 1, #A
    "E0:5A:1B:9C:FD:90": 2, #B
    "CC:50:E3:A8:D9:6C": 3, #C
    "E0:5A:1B:A0:37:D8": 4, #D
    "E0:5A:1B:A0:1F:D0": 5, #E
    "E0:5A:1B:A0:38:C0": 6, #F
    "E0:5A:1B:A0:3D:C8": 7, #G
    "E0:5A:1B:A0:40:F8": 8, #H
    "A4:CF:12:43:6A:A0": 9, #I
    "E0:5A:1B:A0:1E:88": 10, #K
    "80:7D:3A:BC:C6:30": 11, #L
    "E0:5A:1B:A0:1A:C0": 12, #M
    "3C:71:BF:64:3B:74": 13, #O
    "3C:71:BF:62:C2:B8": 14, #P
    "E0:5A:1B:A0:2A:28": 15, #Q
    "E0:5A:1B:A0:4C:84": 16, #R
    "E0:5A:1B:A0:57:0C": 17, #T
    "E0:5A:1B:A0:2F:B8": 18, #U
    "3C:71:BF:63:83:28": 19, #V
    "E0:5A:1B:A0:51:9C": 20, #W
    "3C:71:BF:64:26:74": 21, #X
    
    
    
    
    
    
} #-100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, -100, 
            try:
                cursor = self.db.cursor()
                print("SELECT * FROM cse191.ble_logs WHERE log_ts BETWEEN '{}' AND '{}' GROUP BY device_mac ASC".format(start_time, end_time))
                cursor.execute("SELECT * FROM cse191.ble_logs WHERE log_ts BETWEEN '{}' AND '{}' GROUP BY ble_mac ASC".format(start_time, end_time))
                results = cursor.fetchall()
                # print(results)
                df = pd.DataFrame()
                cur_device = results[0][3]
                rowarray = ["PLACEHOLDER", -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, time]
                for i in range(0, len(results) - 1): 
                    rowarray[0] = results[i][3]
                    while (results[i][3] == cur_device):
                        index = hashmap.get(results[i][1])
                        rowarray[index] = results[i][2]
                        i += 1
                    if results[i][3] != cur_device:
                        cur_device = results[i][3]
                        df = pd.concat([df, pd.DataFrame([rowarray])], ignore_index=True)
                        # reset for next device
                        rowarray = [results[i][3], -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, -85, time]
                        index = hashmap.get(results[i][1])
                        rowarray[index] = results[i][2]
                # append the remaining ones (issue)
                df = pd.concat([df, pd.DataFrame([rowarray])], ignore_index=True)
                df.columns=['DEVICE MAC', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', 'O', 'P', 'Q', 'R', 'T', 'U', 'V', 'W', 'X', "TIME"]
                print(df)
                return df
            except Error as  e:
                print(f"The error '{e}' occurred")
    
    
