import csv
import random
import string
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)

db_host = "localhost"
db_port = "5432"
db_name = "postgres"
db_user = "postgres"
db_password = "123456789"

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
)

cursor = conn.cursor()


def calculate_uptime_downtime(status_data, business_hours_data):
    uptime = timedelta()
    downtime = timedelta()

    for i in range(len(status_data) - 1):
        current_status = status_data[i]["status"]
        next_status = status_data[i + 1]["status"]
        current_timestamp = status_data[i]["timestamp"]
        next_timestamp = status_data[i + 1]["timestamp"]

        start_time = business_hours_data[0][1]
        end_time = business_hours_data[0][2]

        if current_status == "active":
            if next_status == "active":
                uptime += next_timestamp - current_timestamp
            else:
                end_time = min(end_time, next_timestamp.time())
                uptime += (
                    datetime.combine(next_timestamp.date(), end_time)
                    - current_timestamp
                )
        else:
            if next_status == "inactive":
                downtime += next_timestamp - current_timestamp
            else:
                end_time = min(end_time, next_timestamp.time())
                downtime += (
                    datetime.combine(next_timestamp.date(), end_time)
                    - current_timestamp
                )

    uptime = max(uptime, timedelta())  # Ensure uptime is non-negative
    downtime = max(downtime, timedelta())  # Ensure downtime is non-negative

    return uptime, downtime


def interpolate_data_within_business_hours(data, start_time, end_time):
    interpolated_data = []

    current_time = start_time
    while current_time <= end_time:
        interpolated_data.append({"timestamp": current_time, "status": None})
        current_time += timedelta(minutes=1)

    for i in range(len(data) - 1):
        current_entry = data[i]
        next_entry = data[i + 1]

        if current_entry["status"] is None:
            continue

        if next_entry["status"] is None:
            next_entry["status"] = current_entry["status"]

        start_time = current_entry["timestamp"] + timedelta(minutes=1)
        end_time = next_entry["timestamp"]

        for j in range((end_time - start_time).seconds // 60):
            timestamp = start_time + timedelta(minutes=j)
            status = current_entry["status"]
            if status.isdigit():
                status = int(status)
            else:
                status = 0
            interpolated_data[timestamp.minute]["status"] = status

    # Assign default value of 0 for any remaining None values
    for entry in interpolated_data:
        if entry["status"] is None:
            entry["status"] = 0

    return interpolated_data


@app.route("/get_report", methods=["GET"])
def get_report():
    report_id = request.args.get("report_id")

    timezone_query = (
        "SELECT timezone_str FROM timezone_data WHERE report_id = 'LGQMXOS4'"
    )
    cursor.execute(timezone_query, (report_id,))
    timezone_data = cursor.fetchall()

    hours_query = "SELECT day_of_week, start_time_local, end_time_local FROM business_hours_data WHERE report_id = 'LGQMXOS4'"
    cursor.execute(hours_query, (report_id,))
    business_hours_data = cursor.fetchall()

    store_query = """
        SELECT store_id, status, to_char(timestamp_utc, 'YYYY-MM-DD HH24:MI:SS') AS timestamp_utc
        FROM store_data
        WHERE report_id = 'LGQMXOS4' LIMIT 10; 
    """
    cursor.execute(store_query, (report_id,))
    store_data = cursor.fetchall()

    if timezone_data and business_hours_data and store_data:
        if store_data:
            store_id = store_data[0][0]
            status_data = []

            for row in store_data:
                if len(row) >= 3:
                    timestamp = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
                    status = row[1]
                    status_data.append({"timestamp": timestamp, "status": status})

        current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        last_hour_start = current_time - timedelta(hours=1)
        last_day_start = current_time - timedelta(days=1)
        last_week_start = current_time - timedelta(weeks=1)

        uptime, downtime = calculate_uptime_downtime(status_data, business_hours_data)
        print(downtime)
        interpolated_data = interpolate_data_within_business_hours(
            status_data, last_hour_start, current_time
        )
        uptime_last_hour = int(uptime.total_seconds() / 60)
        downtime_last_hour = 60 - uptime_last_hour
        uptime_last_day = sum(
            entry["status"]
            for entry in interpolated_data
            if entry["timestamp"] >= last_day_start
        )
        downtime_last_day = 24 * 60 - uptime_last_day
        uptime_last_week = sum(
            entry["status"]
            for entry in interpolated_data
            if entry["timestamp"] >= last_week_start
        )
        downtime_last_week = 7 * 24 * 60 - uptime_last_week

        report = {
            "store_id": store_id,
            "uptime_last_hour": uptime_last_hour,
            "downtime_last_hour": downtime_last_hour,
            "uptime_last_day": uptime_last_day,
            "downtime_last_day": downtime_last_day,
            "uptime_last_week": uptime_last_week,
            "downtime_last_week": downtime_last_week,
        }
        print(report)
        return jsonify(report)
    else:
        response = jsonify({"error": "Invalid report ID"})

    return response


if __name__ == "__main__":
    app.run()
    cursor.close()
    conn.close()
