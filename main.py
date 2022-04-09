import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd
import quandl
import os


AWS_ACCESS_KEY = os.getenv('ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('SECRET_KEY')
NASDAQ_APIKEY = os.getenv('NASDAQ_APIKEY')


quandl.ApiConfig.api_key = NASDAQ_APIKEY
codes = pd.read_csv('https://static.quandl.com/ECONOMIST_Descriptions/economist_country_codes.csv')



codes_list = codes['COUNTRY|CODE'].tolist()


values = []
codes = []

for country in codes_list:
    country_list = list(country[-3:])
    final_list = (''.join(country_list))
    data = quandl.get('ECONOMIST/BIGMAC_' + str(final_list), start_date='2022-01-31', end_date='2022-01-31')
    if len(data) > 0:
        codes.append(str(final_list))
    df = pd.DataFrame(data).reset_index()
    final = df[df.columns[1:6]]
    values.append(final)

large_df = pd.concat(values, ignore_index=True)
large_df.insert(0, 'code', codes)
large_df.to_csv('BMI_essentials.csv')
bucket = 'bigmac-index'
local_file = 'BMI_essentials.csv'
s3_file = 'BMI_essentials.csv'

#uploading data to s3
def upload_to_aws(local_file, bucket, s3_file):

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

upload_to_aws(local_file, bucket, s3_file)
