import pandas as pd
from google.cloud import bigquery
import json
from google.oauth2 import service_account
import pandas_gbq

creds = service_account.Credentials.from_service_account_file('/home/kaurr_reett/Analysis/key.json')

# Create a client object to connect to BigQuery
client = bigquery.Client(credentials=creds)

# Set the name of the dataset and table to read from
table_ref = client.dataset('ferrous-osprey-375800.HighD').table('ferrous-osprey-375800.HighD.tracksMeta')

query = """
    SELECT *
    FROM `ferrous-osprey-375800.HighD.tracksMeta`
"""

results = client.query(query).result()

# Convert query results to a Pandas DataFrame
df = results.to_dataframe()

car_count = df['class'].value_counts()['Car']
truck_count = df['class'].value_counts()['Truck']

if car_count > truck_count:
    excess_cars = car_count - truck_count
    excess_data = df[df['class'] == 'Car'].tail(excess_cars)
    excess_data_ids = ','.join([str(id) for id in excess_data['id']])
    delete_query = """
        DELETE FROM `ferrous-osprey-375800.HighD.tracksMeta`
        WHERE id IN ({})
    """.format(excess_data_ids)
    client.query(delete_query)
    
elif truck_count > car_count:
    excess_trucks = truck_count - car_count
    excess_data = df[df['class'] == 'Truck'].tail(excess_trucks)
    excess_data_ids = ','.join([str(id) for id in excess_data['id']])
    delete_query = """
        DELETE FROM `ferrous-osprey-375800.HighD.tracksMeta`
        WHERE id IN ({})
    """.format(excess_data_ids)
    client.query(delete_query)


# Run the analysis on the remaining rows in the table
analysis_query = """
SELECT 
  class,
  AVG(meanXVelocity) AS avg_speed,
  CASE 
    WHEN class = 'Car' THEN 35 / AVG(meanXVelocity) -- assume 35 miles per gallon for cars
    WHEN class = 'Truck' THEN 15 / AVG(meanXVelocity) -- assume 15 miles per gallon for trucks
  END AS fuel_efficiency,
  AVG(traveledDistance) AS avg_distance,
  SUM(traveledDistance) AS total_distance,
  COUNT(*) AS num_vehicles,
  SUM(CASE WHEN class = 'Car' THEN traveledDistance*0.345 ELSE traveledDistance * 0.678 END) AS total_emissions
FROM 
  `ferrous-osprey-375800.HighD.tracksMeta`
GROUP BY 
  class
"""

results = client.query(analysis_query).result()

# Convert query results to a Pandas DataFrame
df = results.to_dataframe()

# Execute the analysis query and write the results to a BigQuery table
pandas_gbq.to_gbq(df, destination_table='ferrous-osprey-375800.HighD.analysis_results', project_id='ferrous-osprey-375800', credentials=creds, if_exists='replace')
