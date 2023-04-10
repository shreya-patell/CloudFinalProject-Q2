from google.cloud import pubsub
from google.oauth2 import service_account
import io
import pandas as pd
import os
from glob import glob


project_id = 'project-382903'


credentials = service_account.Credentials.from_service_account_file('creds.json')
publisher = pubsub.PublisherClient(credentials = credentials)


tracks_path = publisher.topic_path(project_id, 'highd-tracks')
tracks_meta_path = publisher.topic_path(project_id, 'highd-tracksMeta')
recording_meta_path = publisher.topic_path(project_id, 'highd-recordingMeta')


files = glob('highd-dataset-v1.0/data/*tracksMeta.csv')
for file in files:
   df = pd.read_csv(file)
   if 'tracksMeta' in file:
       df['recordingId'] = int(file.split('/')[2][0:2])
   tableName = file.split('/')[2].split('_')[1].split('.')[0]
   df['table'] = tableName
   for index, row in df.iterrows():
       topic_path = None
       if tableName == 'tracksMeta':
           topic_path = tracks_meta_path
       elif tableName == 'tracks':
           topic_path = tracks_path
       elif tableName == 'recordingMeta':
           topic_path = recording_meta_path


       json_string = row.to_json()
       future = publisher.publish(topic_path, json_string.encode('utf-8'))
       print(future.result())
