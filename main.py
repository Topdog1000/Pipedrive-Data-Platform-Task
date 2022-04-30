import os
from typing import Optional

import boto3
import pandas as pd
import gzip
import requests
from requests import Response

from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
PIPEDRIVE_ACCESS_TOKEN = os.getenv("PIPEDRIVE_ACCESS_TOKEN")

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

class PipedriveClient:
    # Simple class to interact with pipedrive api
    def __init__(self, url: str, token: str):
        self.session = requests.Session()
        self.params = {"api_token": token}
        self.base_url = url

    def search_deals(self, **kwargs):
        url = f'{self.base_url}/deals/search'
        return self.session.get(url, params={**self.params, **kwargs})

    def create_deal(self, data: dict) -> Response:
        url = f'{self.base_url}/deals'

        return self.session.post(url, params=self.params, json=data)

    def update_deal(self, deal_id: str, data: dict) -> Response:
        url = f'{self.base_url}/deals/{deal_id}'

        return self.session.put(url, params=self.params, json=data)


def download_s3_file(bucket: str, key: str, output: str):
    s3.Bucket(bucket).download_file(Key=key, Filename=output)


def upload_to_pipedrive(df: pd.DataFrame):

    # initialize a client
    client = PipedriveClient('https://eleganceglobal.pipedrive.com/api/v1', PIPEDRIVE_ACCESS_TOKEN)

    for i, row in df.iterrows():
        data = dict(row)
        # search for existing deal
        response = client.search_deals(term=data.get("title"), exact_match=True, fields="title")

        # find deal in returned result
        items = dict(response.json()).get("data", dict()).get("items")
        item: Optional[dict] = next(iter(items or []), None)
        deal = item and item.get("item")

        if deal:
            print("Found deal: ", deal.get("title"))
            # check if "value" in Pipedrive account for the deal is different from calculated new value
            should_update = deal.get("value") != data.get("value")
            if should_update:
                print("Updating deal: ", deal.get("title"))
                client.update_deal(deal_id=deal.get("id"), data={"value": data.get("value")})
        else:
            print("Creating deal: ", data.get("title"), data.get("status"))
            client.create_deal(data)


if __name__ == "__main__":
    file_name = "deals.csv.gz"

    download_s3_file(bucket='pdw-export.zulu', key='test_tasks/deals.csv.gz', output=file_name)

    with gzip.open(file_name, 'rb') as f:
        data = pd.read_csv(f)

    # filter deleted deals
    data = data[data.status != "deleted"]

    # multiply value by 2 for each row
    data["value"] = data["value"].apply(lambda v: v * 2)

    upload_to_pipedrive(data)
