import boto3
import json


def get_alphavantage_apikey():
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=f'alphavantage_config', WithDecryption=True)
    config1 = json.loads(parameter['Parameter']['Value'])
    api_key = config1['apikey']
    
    return api_key

def get_db_credentials():
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=f'timescaledb_credentials', WithDecryption=True)
    config = json.loads(parameter['Parameter']['Value'])
    
    return config

def get_redis_credential():
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=f'redis_credentials', WithDecryption=True)
    config = json.loads(parameter['Parameter']['Value'])
    
    return config