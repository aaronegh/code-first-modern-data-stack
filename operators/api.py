import logging
import json
from time import sleep
import pandas as pd 
from pandas import DataFrame
import dlt
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from utils.destinations import writeToS3


logger = logging.getLogger(__name__)
#TO DO:
# async loading of a generator (yield)
## [optional] encryption in transit - utils to hash field 
# returns a json and loads into destination (raw) subject_timestamp_part_{pagination}.json - skip this and load directly into delta table
# json to deltatable (param: raw path & bronze path)- static method
    # load and flatten all raw path json into deltatable with time based partition date=2024-08-20/subject.deltatable
    # specify level of flattening 
# backfill enabled, time based (support deltatables only) (param: deltatable path) _private function
    # check max datetime in deltatable

# find an api that can load by date, parition by date and key (primary key: id)

class RawExtract:
    def __init__(self,
                 url: str,
                 paginator: object,
                 destination: str = None,
                 incremental_key: str = None,
                 api_auth: object = None,
                 params: dict = None
            ):
            self.destination = destination # keep it here so destination can be shared for read and write
            self.incremental_key = incremental_key # to implement a watermark/checkpoint
            self.paginate_obj = paginate(url=url,
                                         auth=api_auth,
                                         paginator=paginator,
                                         params= params)

            

    @dlt.resource(parallelized=True)
    def _get_data(paginate: object, sleep_in_sec:int):
        for page in paginate:
                sleep(sleep_in_sec)
                if len(page) < 1:
                        logging.info('Source is empty, loading is halt.')
                        break
                yield page
    

    @staticmethod
    def APIKeyAuthenticator(name: str,
                    api_key: str,
                    location: str
        ) -> object:
        auth = APIKeyAuth(name=name, 
                        api_key=api_key, 
                        location=location) 
        return auth
    

    def _dict_response_to_df(self, response: dict) -> DataFrame:
          df = pd.DataFrame.from_dict(response)
          # do some crazy flattening or flattening level to be set here 
          return df
    
    def _dict_response_to_json(self, response: dict) -> DataFrame:
          response_json = json.dumps(response, indent=4)
          return response_json


    #to do: check batch_size_limit, if batch_size_limit is 1mb, it waits till file is 1mb until it writes to file
    def write_to_s3(self, 
                    output_deltalake_table: bool = False,
                    endpoint_url: str = None,
                    aws_access_key_id: str = None,
                    aws_secret_access_key: str = None,
                    aws_region: str = None,
                    bucket_name: str = None, 
                    path_name: str = None
                    ) -> None:
                            
        s3client = writeToS3(endpoint_url,
                    aws_access_key_id,
                    aws_secret_access_key,
                    aws_region,
                    bucket_name,
                    path_name)
        

        s3client.create_bucket()
                            
        try:
            for data in self._get_data(self.paginate_obj,1):
                if output_deltalake_table:
                    result = self._dict_response_to_df(data)
                    s3client.write_to_deltalake(result)

                else:
                    result = self._dict_response_to_json(data)
                    s3client.write_to_json(result)
        
        except Exception as e:
             logging.info(f"Error caught: {e}")
        finally: 
             logging.info('Extraction complete')