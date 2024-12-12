import boto3
import json


def get_alphavantage_apikey():
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=f'alphavantage_config', WithDecryption=True)
    config1 = json.loads(parameter['Parameter']['Value'])
    api_key = config1['apikey']
    
    return api_key