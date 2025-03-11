import boto3
import json


region_name = 'ap-south-1'
ssm = boto3.client('ssm',region_name=region_name)


class Credentials:
    def __init__(self, credential_name=None):
        if credential_name not in ['alpha', 'timescale', 's3']:
            raise ValueError("❌ Invalid credential_name. Choose from: 'alpha', 'timescale', 's3'")
        self.credential_name = credential_name

    def __timescaledb_credentials(self):
        parameter = ssm.get_parameter(Name='timescaledb_credentials', WithDecryption=True)
        timescaledb_config = json.loads(parameter['Parameter']['Value'])
        return timescaledb_config

    def __alphavantage_credentials(self):
        parameter = ssm.get_parameter(Name='alphavantage_config', WithDecryption=True)
        alphavantage_config = json.loads(parameter['Parameter']['Value'])
        return alphavantage_config

    def __s3_credentials(self):
        parameter = ssm.get_parameter(Name='s3_credentials', WithDecryption=True)
        s3_config = json.loads(parameter['Parameter']['Value'])
        return s3_config

    def get_credentials(self):
        if self.credential_name.lower() == "alpha":
            return self.__alphavantage_credentials()
        elif self.credential_name.lower() == "timescale":
            return self.__timescaledb_credentials()
        elif self.credential_name.lower() == "s3":
            return self.__s3_credentials()
        else:
            raise ValueError("❌ No such credentials are available. Choose from: 'alpha', 'timescale', 's3'")




if __name__=='__main__':
    cred = Credentials(credential_name='timescale')
    try:
        print(cred.get_credentials())
    except Exception as e:
        print(f"Error: {e}")
