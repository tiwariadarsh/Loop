import os
import random
import pandas as pd
from flask import Flask, request, jsonify, send_file
from sqlalchemy import create_engine, text

app = Flask(__name__)

# SQLite database setup
# Get the directory of the current script
current_directory = os.path.dirname(os.path.abspath(__file__))

# Define the relative path to the database file
db_relative_path = 'restaurant_data.db'

# Create the full path by joining the current directory and the relative path
db_path = os.path.join(current_directory, db_relative_path)
# Create the database engine
engine = create_engine(f'sqlite:///{db_path}')

# Load data from CSV files into the database
data1 = pd.read_csv('./store status.csv')
data2 = pd.read_csv('./Menu hours.csv')
data3 = pd.read_csv('./bq-results.csv')

data1.to_sql('poll_data', engine, index=False, if_exists='replace')
data2.to_sql('business_hours', engine, index=False, if_exists='replace')
data3.to_sql('store_timezone', engine, index=False, if_exists='replace')

# Helper function to calculate uptime and downtime based on poll data
def calculate_uptime_downtime(store_id, local_timezone):
    # Load data from the database
    conn = engine.connect()
    poll_data = pd.read_sql(text(f'SELECT * FROM poll_data WHERE store_id="{store_id}"'), conn)
    business_hours = pd.read_sql(text(f'SELECT * FROM business_hours WHERE store_id="{store_id}"'), conn)
    
    # Merge poll_data with business_hours
    merged_data = pd.merge(poll_data, business_hours, left_on='store_id', right_on='store_id')
    
    # Convert timestamps to local time
    merged_data['timestamp_local'] = merged_data.apply(lambda row: pd.Timestamp(row['timestamp_utc'], tz='UTC').tz_convert(local_timezone), axis=1)

    # Group data by day and business hours
    grouped_data = merged_data.groupby(['timestamp_local', 'day', 'status']).size().unstack(fill_value=0)

    # Calculate uptime and downtime
    try:
        uptime = grouped_data['active'].sum()
    except KeyError:
        uptime = 24*7*60
    
    try:
        downtime = grouped_data['inactive'].sum()
    except KeyError:
        downtime = 0

    conn.close()

    return uptime, downtime

# Helper function to generate a random report ID
def generate_report_id():
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=10))

# Generate the report with the specified schema
def generate_report(store_id, local_timezone):
    # Calculate uptime and downtime
    uptime_last_hour, downtime_last_hour = calculate_uptime_downtime(store_id, local_timezone)
    uptime_last_day = uptime_last_hour / 60.0
    uptime_last_week = uptime_last_day * 7
    downtime_last_day = downtime_last_hour / 60.0
    downtime_last_week = downtime_last_day * 7

    return {
        "store_id": store_id,
        "uptime_last_hour": uptime_last_hour,
        "uptime_last_day": uptime_last_day,
        "uptime_last_week": uptime_last_week,
        "downtime_last_hour": downtime_last_hour,
        "downtime_last_day": downtime_last_day,
        "downtime_last_week": downtime_last_week
    }

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # Perform calculations to generate the report (as described below)
    # For simplicity, generate a report for a random store
    conn = engine.connect()
    store_id_query = conn.execute(text('SELECT DISTINCT store_id FROM store_timezone'))
    store_ids = [row['store_id'] for row in store_id_query]
    conn.close()

    store_id = random.choice(store_ids)
    local_timezone_query = conn.execute(text(f'SELECT timezone_str FROM store_timezone WHERE store_id="{store_id}"'))
    local_timezone = local_timezone_query.fetchone()['timezone_str']
    report_id = generate_report_id()

    return jsonify({"report_id": report_id})

@app.route('/get_report', methods=['GET'])
def get_report():
    conn = engine.connect()
    store_id = request.args.get('store_id')
    print(store_id)
    # Check if the report exists (for simplicity, assume it's always complete)
    if not store_id:
        return jsonify({"status": "Running"})

    local_timezone_query = conn.execute(text(f'SELECT timezone_str FROM store_timezone WHERE store_id="{store_id}"'))
    local_timezone = local_timezone_query.fetchone()[0]
    print(local_timezone)
    # Generate the report
    report = generate_report(store_id, local_timezone)
    report_df = pd.DataFrame([report])

    # Save the report to a CSV file
    report_csv_path = f'report_{store_id}.csv'
    report_df.to_csv(report_csv_path, index=False)

    return jsonify({"status": "Complete", "csv_file": report_csv_path})

if __name__ == '__main__':
    app.run()
