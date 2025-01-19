##Import packages

##use this when package is not ready
import sys
import os
# Add the root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


import boto3, json, requests, csv
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.alphaintelligence import AlphaIntelligence
from alpha_vantage.fundamentaldata import FundamentalData
from ..cloud.aws.ssm import Credentials   ##use this when package is done

# from SwingedgeCore.cloud.aws.ssm import Credentials
# print("Current sys.path:", sys.path)




class AlphaVantageFunctions:
        def __init__(self,alpha_function_name=None):
                self.alpha_function_name = alpha_function_name
        
        def __getandsetalpha(self):
              cred = Credentials(credential_name='alpha')
              alphavantage_config = cred.get_credentials()
              alpha_api_key = alphavantage_config["apikey"]
              return alpha_api_key
          
        def __set_alpha_functions(self):
            try:
                alphakey = self.__getandsetalpha()
                function_to_return = None
                
                if self.alpha_function_name.lower()=='timeseries':
                    function_to_return = TimeSeries(key=alphakey, output_format="json")
                elif self.alpha_function_name.lower()=='fundamental':
                    function_to_return = FundamentalData(key=alphakey,output_format = 'json')
                elif self.alpha_function_name.lower()=='alphaintelligence':
                    function_to_return = AlphaIntelligence(key=alphakey,output_format='json')
                else:
                    function_to_return = None
                    print("Invalid Alpha function name. Choose from: 'timeseries','fundamental','alphaintelligence'")
                    
                return function_to_return
            except Exception as e:
                raise ValueError("Error getting the Alpha API Key:",e)
            
        def get_alpha_function(self):
            return self.__set_alpha_functions()
        
        def fetch_symbols(self,valid_exchanges=None, valid_asset_types=None):
            alphakey = self.__getandsetalpha()
            CSV_URL = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={alphakey}'
            
            if valid_exchanges is None:
                valid_exchanges = ['NASDAQ', 'NYSE', 'NYSE ARCA']
            if valid_asset_types is None:
                valid_asset_types = ['Stock', 'ETF']
            
            try:
                with requests.Session() as s:
                    print("Fetching data from Alpha Vantage...")
                    download = s.get(CSV_URL)
                    download.raise_for_status()

                    decoded_content = download.content.decode('utf-8')
                    cr = csv.reader(decoded_content.splitlines())
                    header = next(cr)

                    symbol_idx = header.index('symbol')
                    exchange_idx = header.index('exchange')
                    asset_type_idx = header.index('assetType')

                    filtered_symbols = []
                    for row in cr:
                        if row[exchange_idx] in valid_exchanges and row[asset_type_idx] in valid_asset_types:
                            filtered_symbols.append(row[symbol_idx])

                    print(f"Found {len(filtered_symbols)} symbols matching the criteria.")
                    return filtered_symbols

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from Alpha Vantage: {e}")
                return []
            except Exception as e:
                print(f"An error occurred: {e}")
                return []
        


            
if __name__ == '__main__':
    alpha = AlphaVantageFunctions(alpha_function_name='timeseries')
    ts = alpha.get_alpha_function()
    raw_data, metadata = ts.get_intraday(symbol='A', month='2025-01', interval='1min', outputsize='full')

    for i,j in raw_data.items():
        print(i,j)
        break

