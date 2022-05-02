import gzip
import os
from abc import ABC
from typing import Optional

import pandas as pd

import boto3
import requests
from dotenv import load_dotenv
from requests import Response

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
PIPEDRIVE_ACCESS_TOKEN = os.getenv("PIPEDRIVE_ACCESS_TOKEN")


def load(file: str) -> pd.DataFrame:
    with gzip.open(file, 'rb') as f:
        data = pd.read_csv(f)

    return data


class Source(ABC):
    def download(self, **kwargs):
        raise NotImplementedError


class S3(Source):
    def __init__(self, **kwargs):
        self.s3 = boto3.resource(
            service_name='s3',
            region_name='us-east-1',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        self.bucket = kwargs.pop("bucket")

    def download(self, key: str, output: str):
        self.s3.Bucket(self.bucket).download_file(Key=key, Filename=output)


class HTTP(Source):
    def download(self):
        pass


class Destination(ABC):
    def upload(self, **kwargs):
        raise NotImplementedError


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


class PipeDrive(Destination):

    def __init__(self):
        self.client = PipedriveClient('https://eleganceglobal.pipedrive.com/api/v1', PIPEDRIVE_ACCESS_TOKEN)

    def upload(self, file: str):
        df = load(file)

        # filter deleted deals
        df = df[df.status != "deleted"]

        # multiply value by 2
        df["value"] = df["value"].apply(lambda v: v * 2)

        for i, row in df.iterrows():
            data = dict(row)
            # search for existing deal
            response = self.client.search_deals(term=data.get("title"), exact_match=True, fields="title")
            deal: Optional[dict] = None

            if response.ok:
                items = dict(response.json()).get("data", dict()).get("items")
                item: Optional[dict] = next(iter(items or []), None)
                deal = item and item.get("item")

            if deal:
                print("Found deal: ", deal.get("title"))
                should_update = deal.get("value") != data.get("value")
                if should_update:
                    print("Updating deal")
                    self.client.update_deal(deal_id=deal.get("id"), data=deal)
            else:
                print("Creating deal: ", data.get("title"), data.get("status"))
                self.client.create_deal(data)


class Postgres(Destination):
    def upload(self):
        pass


if __name__ == "__main__":
    file_name = "deals.csv.gz"

    source = S3(bucket="pdw-export.zulu")
    source.download("test_tasks/deals.csv.gz", file_name)

    destination = PipeDrive()
    destination.upload(file_name)
