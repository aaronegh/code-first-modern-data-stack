from operators.api import RawExtract
from utils.paginators import QueryParamPaginator

# to do: load from env
auth = RawExtract.APIKeyAuthenticator(name="x-api-key", api_key="live_GVLvarCVqkXNrm6Zk6aW5g8VaghA1VJ8Ie9AjJW23hhV0ZkpIwhWZdrAdwMRntmg", location="header")
url = 'https://api.thecatapi.com/v1/images/search'
paginator=QueryParamPaginator(page_param="page", initial_page=1)
params={
        "limit":1,
        "has_breads":1,
        "breed_ids":"abys",
        "order": "DESC"
    }

endpoint_url='http://localhost:9000'
aws_access_key_id='minioadmin'
aws_secret_access_key='minioadmin'
aws_region= "us-west-2"
bucket_name="delta-bucket"
path_name="dlt-table"


cat_api = RawExtract(url=url,
                     paginator=paginator,
                     api_auth=auth,
                     params=params
                    )


cat_api.write_to_s3(output_deltalake_table=True,
                    endpoint_url=endpoint_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_region=aws_region,
                    bucket_name=bucket_name,
                    path_name=path_name
                    )


