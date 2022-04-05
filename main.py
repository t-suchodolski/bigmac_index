import boto3
from botocore.exceptions import NoCredentialsError
import wget
import pandas as pd

import nasdaqdatalink
import quandl
import os


AWS_ACCESS_KEY = os.getenv('ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('SECRET_KEY')
NASDAQ_APIKEY = os.getenv('NASDAQ_APIKEY')

quandl.ApiConfig.api_key = 'SY39_7QTBxtjE5topf6Q'

#fetching country codes from economis_country_codes.csv
data = quandl.get('ECONOMIST/BIGMAC_ROU', start_date='2022-01-31', end_date='2022-01-31')
df = pd.DataFrame(data)
final = df[df.columns[1:6]]
print(final)

#df.to_csv('romania.csv')

#creating list with 3-letters country ID which are then used to create distinct link for each country data
#index_list = data['COUNTRY|CODE'].tolist()


#uploading data to s3
def upload_to_aws(local_file, bucket, s3_file):
    bucket = 'bigmac-index'
    local_file = file
    s3_file = s3_filename
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



#uploading data to hard drive so it can be transfered to s3, deleting it afterwards
def transfer():
    for country in index_list:
        country_list = list(country[-3:])
        final_list = (''.join(country_list))
        file = wget.download('https://data.nasdaq.com/api/v3/datasets/ECONOMIST/BIGMAC_' + str(
            ''.join(country_list)) + '.csv?api_key=SY39_7QTBxtjE5topf6Q')
        s3_filename = ('BIGMAC_' + str(''.join(country_list)) + '.csv')
        uploaded = upload_to_aws('local_file', 'bucket_name', 's3_file_name')
        os.remove('ECONOMIST-BIGMAC_' + str(''.join(country_list)) + '.csv')

    os.remove('economist_country_codes.csv')