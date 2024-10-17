import boto3
import pandas as pd
import os
import gzip
from dynamodb_json import json_util as json_b
import swifter


import uuid


# AWS credentials and bucket names
aws_access_key_id = ''
aws_secret_access_key = ''
source_bucket_name = ''
target_bucketc_name =''


# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# SUB FOLDER LOCATION {Dynaomdb Extraction Location}
folders = [
    # 'Table-Name/AWSDynamoDB/0172185664-e44640de/data/'
]

def uuid_version(uuid_string):
    try:
        uuid_obj = uuid.UUID(uuid_string)
        return uuid_obj.version
    except ValueError:
        return 'None'

def decompress_gzip_file(input_file, output_file):
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            f_out.write(f_in.read())


def ddb_json_to_json(rec):
    try :
        data = json_b.loads(rec)
        return data
    except :
        return None

def process_data_file(input_file, count):

    # Load data from JSON file
    df = pd.read_json(input_file, lines=True)
    df['Item'] = df['Item'].swifter.apply(ddb_json_to_json)
    # item_column = df['Item']
    item_df = df['Item'].swifter.apply(pd.Series)

# Define Schema Here
    item_df = item_df.astype(str)
    item_df = item_df.fillna('-')
    item_df = item_df.replace('-11111', '-')
    item_df = item_df.replace('nan', '-')


# Writing Data to parquet
    file_name = f'data{count}.parquet'
    item_df.to_parquet(file_name)

    return file_name



def upload_to_s3(file_name, key):
    s3.upload_file(file_name, target_bucketc_name, key)
    print("file upload success :",file_name)


def main():

    for source_folder in folders :

        # List files in source folder
        response = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_folder)
        count =1

        # Process each file
        for obj in response['Contents']:
            file_key = obj['Key']
            print(file_key)
            filename = os.path.basename(file_key)

            # Download file from S3
            local_file =  filename
            s3.download_file(source_bucket_name, file_key, local_file)

            # Decompress the file if it's in .gz format
            if filename.endswith('.gz'):
                decompressed_file = local_file[:-3]
                decompress_gzip_file(local_file, decompressed_file)
                os.remove(local_file)
                local_file = decompressed_file

# Process the data
            target_file_name = process_data_file(local_file,count)
            count =count+1


# Upload the file to the destination S3 bucket
            destination_key = f"/{target_file_name}"
            upload_to_s3(target_file_name, destination_key)


# Remove the files from local storage
            # os.remove(local_file)
            # os.remove(target_file_name)


if __name__ == "__main__":
    main()
