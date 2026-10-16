from datetime import datetime
import json
import os

from dotenv import load_dotenv
from flask import Flask
from tabulate import tabulate
import pandas as pd
import requests

load_dotenv()

app = Flask(__name__)

def get_departure_board():

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
        "code": "LDS",
        "timeWindow": 60,
        }

    response = requests.get(url, headers=headers, params=params)

    services = []
    station_full_name = ""

    if 'error' not in response:
        response_json = json.loads(response.text)
        station_full_name = response_json["query"]["location"]["description"]

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
                                f"+{str(delay_minutes)}" if delay_minutes > 0 else "",
                                number_of_coaches,
                                f"Cancelled ({cancellation_reason})" if cancelled == True else ""])
    
    return services, station_full_name
    
@app.route("/")
def dashboard():
    services, station_full_name = get_departure_board()

    # Format your data as an HTML table
    table = tabulate(
        services, headers=["Platform", 
                                      "Operator", 
                                      "Destination", 
                                      "Scheduled",
                                      "Estimated",
                                      "Delay",
                                      "Coaches",
                                      "Status"],
        tablefmt="html",
        colalign=("left",) * len(services[0])
    )

    now = datetime.now().strftime("%H:%M:%S")

    return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
            <meta name="viewport"
                content="width=device-width, initial-scale=1">
            <meta http-equiv="refresh" content="60">
            <title>{station_full_name} Departure Board</title>
            <style>
                body {{
                    # font-family: Arial, sans-serif;
                    font-family: 'Space Mono', monospace;
                    padding: 20px;
                    background: #f4f6f8;
                }}
                # h1 {{ font-size: 28px; }}
                p {{ color: #555; }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    background: white;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 14px;
                    text-align: left;
                }}
                td:nth-child(6) {{
                    color: red;
                }}
                th {{
                    background: #263746;
                    color: white;
                }}
                tr:nth-child(even) {{
                    background: #edf2f7;
                }}
            </style>
        </head>
        <body>
            <h1>{station_full_name}</h1>
            <h2>Departure</h2>
            <p>Last updated: {now}</p>
            {table}
        </body>
        </html>
    """

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)