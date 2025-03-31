from datetime import datetime
from dotenv import dotenv_values
import gspread
import pandas as pd
import pprint
import requests

def calculate_delay_repay(operator, delay):
    if (((operator == 'CrossCountry') & (delay >= 30))
        | ((operator == 'LNER') & (delay >= 30))
        | ((operator == 'Northern') & (delay >= 15))
        | ((operator == 'East Midlands Railway') & (delay >= 15))
        | ((operator == 'Transpennine Express') & (delay >= 15))
        | ((operator == 'Great Western Railway') & (delay >= 15))):
        return 'Y'
    return 'N'

#Define API user details
my_secrets = dotenv_values(".env")
api_username = my_secrets['API_USERNAME']
api_password = my_secrets['API_PASSWORD']

gc = gspread.service_account()
sh = gc.open('My_Train_Journeys_New_2')
worksheet = sh.get_worksheet(0)
my_train_journeys = pd.DataFrame(worksheet.get_all_records())
my_train_journeys_to_process = my_train_journeys.loc[my_train_journeys['Processed'] == '']

for row, journey in my_train_journeys_to_process.iterrows():
    print(row)
    service_uid         = journey['Service UID:']
    date_of_travel      = datetime.strptime(journey['Date:'], '%d/%m/%Y').strftime('%Y/%m/%d')
    boarded_at          = journey['Boarded at:']
    alighted_at         = journey['Alighted at:']
    class_number        = journey['Class']
    number_of_coaches   = journey['Number of coaches:']
    print(service_uid, (date_of_travel), boarded_at, alighted_at, class_number, number_of_coaches)

    response = requests.get(f'https://api.rtt.io/api/v1/json/service/{service_uid}/{date_of_travel}', auth=(api_username, api_password)).json()

    if 'error' not in response:
        operator = response['atocName']

        origin                          = response['locations'][0]['crs']
        origin_scheduled_departure_time = response['locations'][0]['gbttBookedDeparture']
        if 'cancelReasonCode' in response['locations'][0]:
            origin_real_departure_time  = '0000'
        else:
            origin_real_departure_time  = response['locations'][0]['realtimeDeparture']

        dest                        = response['locations'][-1]['crs']
        dest_scheduled_arrival_time = response['locations'][-1]['gbttBookedArrival']
        if 'cancelReasonCode' in response['locations'][-1]:
            dest_real_arrival_time  = '0000'
        else:
            dest_real_arrival_time  = response['locations'][-1]['realtimeArrival']

        boarded_at_stop_number              = -1
        boarded_at_scheduled_departure_time = '0000'
        boarded_at_real_departure_time      = '0000'

        alighted_at_stop_number             = -1
        alighted_at_scheduled_arrival_time  = '0000'
        alighted_at_real_arrival_time       = '0000'

        for stop_info in response['locations']:
            if stop_info['crs'] == boarded_at:
                boarded_at_stop_number = response['locations'].index(stop_info)
                boarded_at_scheduled_departure_time = stop_info['gbttBookedDeparture']
                boarded_at_real_departure_time = stop_info['realtimeDeparture']

            if stop_info['crs'] == alighted_at:
                alighted_at_stop_number = response['locations'].index(stop_info)
                alighted_at_scheduled_arrival_time = stop_info['gbttBookedArrival']
                alighted_at_real_arrival_time = stop_info['realtimeArrival']
                delay = int((datetime.strptime(alighted_at_real_arrival_time, '%H%M') - datetime.strptime(alighted_at_scheduled_arrival_time, '%H%M')).total_seconds() / 60)
                delay_repay = calculate_delay_repay(operator, delay)

        print('Operator ' + operator)
        print('Origin ' + origin + ' at ' + origin_scheduled_departure_time + ' / ' + origin_real_departure_time)
        print('Boarded at station number ' + str(boarded_at_stop_number) + ' at ' + boarded_at_real_departure_time + ' / ' + boarded_at_scheduled_departure_time)
        print('Alighting at station number ' + str(alighted_at_stop_number) + ' at ' + alighted_at_scheduled_arrival_time + ' / ' + alighted_at_real_arrival_time)
        print('Destination ' + dest + ' at ' + dest_scheduled_arrival_time + ' / ' + dest_real_arrival_time)

        if ((boarded_at_stop_number > -1) & (alighted_at_stop_number > -1) & (boarded_at_stop_number < alighted_at_stop_number)):
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
            print(journey)

            worksheet.update_cell(row + 2, 6, operator)
            worksheet.update_cell(row + 2, 7, origin)
            worksheet.update_cell(row + 2, 8, '\'' + str(origin_scheduled_departure_time))
            worksheet.update_cell(row + 2, 9, '\'' + str(origin_real_departure_time))
            # worksheet.update_cell(row + 2, 12, boarded_at)
            worksheet.update_cell(row + 2, 11, '\'' + str(boarded_at_scheduled_departure_time))
            worksheet.update_cell(row + 2, 12, '\'' + str(boarded_at_real_departure_time))
            # worksheet.update_cell(row + 2, 15, alighted_at)
            worksheet.update_cell(row + 2, 14, '\'' + str(alighted_at_scheduled_arrival_time))
            worksheet.update_cell(row + 2, 15, '\'' + str(alighted_at_real_arrival_time))
            worksheet.update_cell(row + 2, 16, dest)
            worksheet.update_cell(row + 2, 17, '\'' + str(dest_scheduled_arrival_time))
            worksheet.update_cell(row + 2, 18, '\'' + str(dest_real_arrival_time))
            worksheet.update_cell(row + 2, 19, delay)
            worksheet.update_cell(row + 2, 20, delay_repay)
            worksheet.update_cell(row + 2, 21, 'Y')

            print('Updated!')
            

        







