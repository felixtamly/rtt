from datetime import datetime
import json
import os

from dotenv import load_dotenv
from tabulate import tabulate
import pandas as pd
import requests

load_dotenv()

RED = "\033[91m"
RESET = "\033[0m"

url_access = "https://data.rtt.io/api/get_access_token"

#Define API user details
api_token = os.getenv("API_TOKEN")
headers_access = {"Authorization": f"Bearer {api_token}"}
response_access = requests.get(url_access, headers=headers_access)

data_access = response_access.json()

access_token = data_access["token"]

url = "https://data.rtt.io/gb-nr/location"

headers = {"Authorization": f"Bearer {access_token}"}

params = {
    "code": "HGT",
    "timeFrom": "2026-10-16T09:05:00",
    "timeWindow": 5
    }

response = requests.get(url, headers=headers, params=params)

if 'error' not in response:
    response_json = json.loads(response.text)

    print(json.dumps(response_json, indent=4))

    # quit()

    print(f"{response_json["query"]["location"]["description"]} Departure Board")

    services = []

    if "services" in response_json:
        for service in response_json["services"]:
            if "departure" in service["temporalData"]:
                destination = service["destination"][0]["location"]["description"]
                scheduled_departure_time = service["temporalData"]["departure"]["scheduleAdvertised"]
                scheduled_departure_time_print = scheduled_departure_time.split('T')[1][:5]
                if "realtimeForecast" in service["temporalData"]["departure"]:
                    estimated_departure_time = service["temporalData"]["departure"]["realtimeForecast"]
                    estimated_departure_time_print = estimated_departure_time.split('T')[1][:5]
                else:
                    estimated_departure_time = scheduled_departure_time
                    estimated_departure_time_print = scheduled_departure_time_print

                scheduled_departure_time_dt = datetime.fromisoformat(scheduled_departure_time)
                estimated_departure_time_dt = datetime.fromisoformat(estimated_departure_time)

                if estimated_departure_time_dt > scheduled_departure_time_dt:
                    delay = estimated_departure_time_dt - scheduled_departure_time_dt
                    delay_minutes = int(delay.total_seconds() / 60)
                else:
                    delay_minutes = 0

                cancelled = service["temporalData"]["departure"]["isCancelled"]
                if cancelled == True:
                    if "cancellationReasonCode" in service["temporalData"]["departure"]:
                        cancellation_reason_code = service["temporalData"]["departure"]["cancellationReasonCode"]
                        for reason in response_json["reasons"]:
                            if reason["code"] == cancellation_reason_code:
                                cancellation_reason = reason["longText"]

                operator = service["scheduleMetadata"]["operator"]["name"]
                if "platform" in service["locationMetadata"]:
                    if "actual" in service["locationMetadata"]["platform"]:
                        platform = service["locationMetadata"]["platform"]["actual"]
                    else:
                        platform = service["locationMetadata"]["platform"]["planned"]
                elif "line" in service["locationMetadata"]:
                    platform = service["locationMetadata"]["line"]["planned"]
                else:
                    platform = "--"
                if "numberOfVehicles" in service["locationMetadata"]:
                    number_of_coaches = service["locationMetadata"]["numberOfVehicles"]
                else:
                    number_of_coaches = "--"

                services.append([platform, 
                                operator, 
                                destination, 
                                scheduled_departure_time_print, 
                                estimated_departure_time_print, 
                                f"{RED}+{str(delay_minutes)}{RESET}" if delay_minutes > 0 else "",
                                number_of_coaches,
                                f"{RED}Cancelled ({cancellation_reason}){RESET}" if cancelled == True else ""])
    
    print(tabulate(services, headers=["Platform", 
                                      "Operator", 
                                      "Destination", 
                                      "Scheduled",
                                      "Estimated",
                                      "Delay",
                                      "Coaches",
                                      "Status"], tablefmt="grid"))

