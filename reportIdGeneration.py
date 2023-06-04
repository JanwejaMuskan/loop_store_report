import csv
import random
import string
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)

db_host = 'localhost'
db_port = '5432'
db_name = 'postgres'
db_user = 'postgres'
db_password = '123456789'

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)

cursor = conn.cursor()

# Generate a random report ID
def generate_report_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))



# Endpoint to trigger report generation
@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    try:
        # Generate a random report ID
        report_id = generate_report_id()

        # Add the report ID to the store_data table
        update_query = """
            UPDATE store_data
            SET report_id = %s
          """

        cursor.execute(update_query, (report_id,))
        conn.commit()

        # Add the report ID to the business_hours_data table
        update_query = """
            UPDATE business_hours_data
            SET report_id = %s
          """

        cursor.execute(update_query, (report_id,))
        conn.commit()

        # Add the report ID to the timezone_data table
        update_query = """
            UPDATE timezone_data
            SET report_id = %s
          """

        cursor.execute(update_query, (report_id,))
        conn.commit()

        # Start a background task to generate the report based on the data in the database
        # In this example, we will simply store the report ID in the database for demonstration purposes

        return jsonify({'report_id': report_id})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run()
