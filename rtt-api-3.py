from datetime import datetime
from dotenv import dotenv_values
from gspread_formatting import *
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
import pprint
import requests
import os
import json

def calculate_delay_repay(operator, delay):
    if (((operator == 'CrossCountry') & (delay >= 30))
        | ((operator == 'LNER') & (delay >= 30))
        | ((operator == 'Northern') & (delay >= 15))
        | ((operator == 'East Midlands Railway') & (delay >= 15))
        | ((operator == 'Transpennine Express') & (delay >= 15))
        | ((operator == 'Great Western Railway') & (delay >= 15))
        | ((operator == 'South Western Railway') & (delay >= 15))
        | ((operator == 'Transport for Wales') & (delay >= 15))
        | ((operator == 'Southern') & (delay >= 15))
        | ((operator == 'ScotRail') & (delay >= 30))):
        return 'Y'
    return 'N'

url_access = "https://data.rtt.io/api/get_access_token"

#Define API user details
#Local
# my_secrets = dotenv_values(".env")
# api_token = my_secrets['API_TOKEN']
#Github Actions
api_token = os.environ["API_TOKEN"]
headers_access = {"Authorization": f"Bearer {api_token}"}
response_access = requests.get(url_access, headers=headers_access)

data_access = response_access.json()
access_token = data_access["token"]

#Define API user details (Github Actions)
scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

#Local
# gc = gspread.service_account()
# sh = gc.open('My_Train_Journeys_New_2')
#Github Actions
creds = Credentials.from_service_account_file(
    "creds.json",
    scopes=scopes
)
gc = gspread.authorize(creds)
sh = gc.open("My_Train_Journeys_New_2")
worksheet = sh.get_worksheet(0)
my_train_journeys = pd.DataFrame(worksheet.get_all_records())
my_train_journeys_to_process = my_train_journeys.loc[my_train_journeys['Processed'] == '']

for row, journey in my_train_journeys_to_process.iterrows():
    service_uid         = journey['Service UID']
    date_of_travel      = datetime.strptime(journey['Date'], '%d/%m/%Y').strftime('%Y-%m-%d')
    boarded_at          = journey['Boarded at']
    alighted_at         = journey['Alighted at']
    class_number        = journey['Class']
    number_of_coaches   = journey['Number of coaches']
    print(service_uid, (date_of_travel), boarded_at, alighted_at, class_number, number_of_coaches)

    url = "https://data.rtt.io/gb-nr/service"

    headers = {"Authorization": f"Bearer {access_token}"}

    params = {
        "identity": {service_uid},
        "departureDate": {date_of_travel}
    }

    response = requests.get(url, headers=headers, params=params)

    if 'error' not in response:
        response_json = json.loads(response.text)
        operator = response_json["service"]["scheduleMetadata"]["operator"]["name"]

        origin                          = response_json["service"]["locations"][0]["location"]["shortCodes"][0]
        origin_scheduled_departure_time = datetime.fromisoformat(response_json["service"]['locations'][0]["temporalData"]["departure"]["scheduleAdvertised"]).strftime("%H%M")

        if response_json["service"]["locations"][0]["temporalData"]["departure"]["isCancelled"]:
            origin_real_departure_time  = "0000"
        else:
            origin_real_departure_time  = datetime.fromisoformat(response_json["service"]['locations'][0]["temporalData"]["departure"]["realtimeActual"]).strftime("%H%M")
    

        dest                        = response_json["service"]["locations"][-1]["location"]["shortCodes"][0]
        dest_scheduled_arrival_time = datetime.fromisoformat(response_json["service"]['locations'][-1]["temporalData"]["arrival"]["scheduleAdvertised"]).strftime("%H%M")
    
        if response_json["service"]["locations"][-1]["temporalData"]["arrival"]["isCancelled"]:
            dest_real_arrival_time  = '0000'
        else:
            dest_real_arrival_time  = datetime.fromisoformat(response_json["service"]['locations'][-1]["temporalData"]["arrival"]["realtimeActual"]).strftime("%H%M")

        boarded_at_stop_number              = -1
        boarded_at_scheduled_departure_time = '0000'
        boarded_at_real_departure_time      = '0000'

        alighted_at_stop_number             = -1
        alighted_at_scheduled_arrival_time  = '0000'
        alighted_at_real_arrival_time       = '0000'

        for stop_info in response_json["service"]["locations"]:
            if stop_info["location"]["shortCodes"][0] == boarded_at:
                boarded_at_stop_number = response_json["service"]["locations"].index(stop_info)
                boarded_at_scheduled_departure_time = datetime.fromisoformat(stop_info["temporalData"]["departure"]["scheduleAdvertised"]).strftime("%H%M")
                boarded_at_real_departure_time = datetime.fromisoformat(stop_info["temporalData"]["departure"]["realtimeActual"]).strftime("%H%M")

            if stop_info["location"]["shortCodes"][0] == alighted_at:
                alighted_at_stop_number = response_json["service"]["locations"].index(stop_info)
                alighted_at_scheduled_arrival_time = datetime.fromisoformat(stop_info["temporalData"]["arrival"]["scheduleAdvertised"]).strftime("%H%M")
                alighted_at_real_arrival_time = datetime.fromisoformat(stop_info["temporalData"]["arrival"]["realtimeActual"]).strftime("%H%M")
                delay = int((datetime.strptime(alighted_at_real_arrival_time, '%H%M') - datetime.strptime(alighted_at_scheduled_arrival_time, '%H%M')).total_seconds() / 60)
                delay_repay = calculate_delay_repay(operator, delay)

        print('Operator ' + operator)
        print('Origin ' + origin + ' at ' + origin_scheduled_departure_time + ' / ' + origin_real_departure_time)
        print('Boarded at station number ' + str(boarded_at_stop_number) + ' at ' + boarded_at_real_departure_time + ' / ' + boarded_at_scheduled_departure_time)
        print('Alighting at station number ' + str(alighted_at_stop_number) + ' at ' + alighted_at_scheduled_arrival_time + ' / ' + alighted_at_real_arrival_time)
        print('Destination ' + dest + ' at ' + dest_scheduled_arrival_time + ' / ' + dest_real_arrival_time)

        if ((boarded_at_stop_number > -1) & (alighted_at_stop_number > -1) & (boarded_at_stop_number < alighted_at_stop_number)):
            row_to_update = "F" + str(row + 2)
            journey['Operator']     = operator
            journey['ORG']          = origin
            journey['ORG-ST']       = origin_scheduled_departure_time
            journey['ORG-RT']       = origin_real_departure_time
            journey['From']         = boarded_at
            journey['From-ST']      = boarded_at_scheduled_departure_time
            journey['From-RT']      = boarded_at_real_departure_time
            journey['To']           = alighted_at
            journey['To-ST']        = alighted_at_scheduled_arrival_time
            journey['To-RT']        = alighted_at_real_arrival_time
            journey['DES']          = dest
            journey['DES-ST']       = dest_scheduled_arrival_time
            journey['DES-RT']       = dest_real_arrival_time
            journey['Delay (min)']  = delay
            journey['Delay Repay']  = delay_repay
            journey['Processed']    = 'Y'

            worksheet.update(range_name=row_to_update, values=[[
                operator
                , origin
                , origin_scheduled_departure_time
                , origin_real_departure_time
                , boarded_at
                , boarded_at_scheduled_departure_time
                , boarded_at_real_departure_time
                , alighted_at
                , alighted_at_scheduled_arrival_time
                , alighted_at_real_arrival_time
                , dest
                , dest_scheduled_arrival_time
                , dest_real_arrival_time
                , delay
                , delay_repay
                , 'Y'
            ]])
            worksheet.format(str(row + 2) + ":" + str(row + 2), {"horizontalAlignment": "LEFT"})
            date_format = CellFormat(
                    numberFormat=NumberFormat(type='DATE', pattern='yyyy-MM-dd')
                )
            format_cell_range(worksheet, "C" + str(row + 2), date_format)

            print('Updated!')
