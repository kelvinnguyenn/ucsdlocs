import json
import pandas as pd
import pytz
from botocore.vendored import requests
import boto3
import datetime
import os

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    # Get request
    r = requests.get('https://waitz.io/live/ucsd')
    # Define data
    d = r.json()['data']
    # Create dataframe from JSON & get relevant columns for data analysis
    df = pd.DataFrame(d)[['name', 'id', 'busyness', 'people', 'isAvailable', 'capacity', 'hourSummary', 'isOpen', 'bestLocations', 'percentage', 'subLocs']]
    # Define timestamp
    pst = pytz.timezone('America/Los_Angeles')

    timestamp = datetime.datetime.now(tz=pst).strftime("%Y-%m-%d %H:%M:")
    # Gets Geisel, both gyms, and Biomedical Library (Filtering data essentially & change ids)

    df = df[(df['name'] == 'Geisel Library') | 
            (df['name'] == 'Main Gym') | 
            (df['name'] == 'Price Center') | 
            (df['name'] == 'WongAvery Library') | 
            (df['name'] == 'RIMAC Fitness Gym') |
            (df['name'] == 'WongAvery Grad Study')].reset_index().drop(columns=['index'])
    # Change ids such that they are now unique
    df.at[0, 'id'] = 16
    df.at[2, 'id'] = 18
    df.at[5, 'id'] = 19
    
    # Add timestamp column
    df['timestamp'] = timestamp
    
    fieldnames = ['timestamp', 'busyness', 'people', 'id', 'percentage']
    bucket_name = 'geiselbucket'
    
    # Use for sublocations
    def subloc_generator(location, timestamp, filepath):
        loc_sublocs = pd.DataFrame(location['subLocs'].iloc[0])
        loc_sublocs['timestamp'] = timestamp
        
        for _, row in loc_sublocs.iterrows():
            floor = row['abbreviation']
            floor_fp = f'{filepath}/{filepath}_{floor}' + '.csv'
            
            try:
                s3.head_object(Bucket=bucket_name, Key=floor_fp)
                file_exists = True
            except:
                file_exists = False
            
            if file_exists:
                data = row[fieldnames]
                
                # Create new line
                new_line = ','.join(map(str, data))
                
                # Retrieve existing file
                response = s3.get_object(Bucket=bucket_name, Key=floor_fp)
                existing_data = response['Body'].read().decode('utf-8')
                
                # Append new line to existing file
                updated_data = existing_data + '\n' + new_line
                
                # Upload updated file back to S3
                s3.put_object(Body=updated_data, Bucket=bucket_name, Key=floor_fp)
            else:
                # Create new file
                new_data = pd.DataFrame([row[fieldnames]]).to_csv(index=False)
                s3.put_object(Body=new_data, Bucket=bucket_name, Key=floor_fp)
    # Use this for bestLocs & general main locations
    def main_generator(location, timestamp, filepath, fieldnames):
        location_name = location['name'].str.lower().str.split().str[0].iloc[0]
        loc_fp = f'{filepath}/{location_name}_main' + '.csv'
        
        try:
            s3.head_object(Bucket=bucket_name, Key=loc_fp)
            file_exists = True
        except:
            file_exists = False
            
        if file_exists:
            data = location[fieldnames]
            print(data)
            # Create new line
            new_line = ','.join(map(str, data.iloc[0]))
            
            # Retrieve existing file
            response = s3.get_object(Bucket=bucket_name, Key=loc_fp)
            existing_data = response['Body'].read().decode('utf-8')
            
            # Append new line to existing file
            updated_data = existing_data + '\n' + new_line
            
            # Upload updated file back to S3
            s3.put_object(Body=updated_data, Bucket=bucket_name, Key=loc_fp)
        else:
            # Create new file
            new_data = location[fieldnames].to_csv(index=False)
            s3.put_object(Body=new_data, Bucket=bucket_name, Key=loc_fp)
        
        
    
    # Geisel Library
    geisel_df = df[df['name'] == 'Geisel Library']
    
    main_generator(geisel_df, timestamp, 'geisel', fieldnames)
    # Generate files for Geisel subfloors
    subloc_generator(geisel_df, timestamp, 'geisel')
    print("Geisel completed")
    # Biomed Library
    biomed_df = df[df['name'] == 'WongAvery Library']
    
    main_generator(biomed_df, timestamp, 'biomed', fieldnames)
    
    subloc_generator(biomed_df, timestamp, 'biomed')
    print("Biomed completed")
    # Price Center
    price_df = df[df['name'] == 'Price Center']
    
    main_generator(price_df, timestamp, 'price', fieldnames)
    
    subloc_generator(price_df, timestamp, 'price')
    print("Price completed")
    # RIMAC
    rimac_df = df[df['name'] == 'RIMAC Fitness Gym']
    
    main_generator(rimac_df, timestamp, 'rimac', fieldnames)
    print("RIMAC completed")
    # Main Gym
    mg_df = df[df['name'] == 'Main Gym']
    
    main_generator(mg_df, timestamp, 'gyms', fieldnames)
    print("Main completed")
    # Best Locations for Locations with Best Locations
    bl_fieldnames = 'abbreviation'
    # TBH, we only really care about Geisel for bestLocs (not necessary for 
    # a new function)
    # Geisel
    bestlocations = ',\n'.join([i['abbreviation'] for i in geisel_df['bestLocations'][0]]) + ',\n'
    geisel_best_fp = 'bestLocs/geisel.csv'
    
    try:
        s3.head_object(Bucket=bucket_name, Key=geisel_best_fp)
        file_exists = True
    except:
        file_exists = False
        
    if file_exists:
        # Retrieve existing file
        response = s3.get_object(Bucket=bucket_name, Key=geisel_best_fp)
        existing_data = response['Body'].read().decode('utf-8')
        
        # Append new line to existing file
        updated_data = existing_data + bestlocations
        
        # Upload updated file back to S3
        s3.put_object(Body=updated_data, Bucket=bucket_name, Key=geisel_best_fp)
    else:
        new_data = 'abbreviation\n' + bestlocations
        s3.put_object(Body=new_data, Bucket=bucket_name, Key=geisel_best_fp)
    print("Geisel bestLocs completed")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

