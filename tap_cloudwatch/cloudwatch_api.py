import os
import time
from datetime import datetime

import boto3


class CloudwatchAPI:

    def __init__(self):
        self._client = None

    @property
    def client(self):
        if not self._client:
            raise Exception('Client not yet initialized')
        return self._client

    def authenticate(self, config):
        self._client = self._create_client(config)

    def _create_client(self, config):
        aws_access_key_id = config.get(
            'aws_access_key_id'
        ) or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get(
            'aws_secret_access_key'
        ) or os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = config.get(
            'aws_session_token'
        ) or os.environ.get('AWS_SESSION_TOKEN')
        aws_profile = config.get('aws_profile') or os.environ.get('AWS_PROFILE')
        aws_endpoint_url = config.get('aws_endpoint_url')
        aws_region_name = config.get('aws_region_name')

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=aws_region_name,
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)
        if aws_endpoint_url:
            logs = aws_session.client('logs', endpoint_url=aws_endpoint_url)
        else:
            logs = aws_session.client('logs')
        return logs

    def get_records(self, bookmark, log_group, query, start_date):
        # TODO: managed batching
        start_query_response = self.client.start_query(
            logGroupName=log_group,
            startTime=int(datetime.strptime(start_date, "%Y-%m-%d").timestamp()),
            # startTime=int((datetime.today() - timedelta(hours=5)).timestamp()),
            endTime=int(datetime.utcnow().timestamp()),
            queryString=query,
            limit=50
        )

        # | sort @timestamp desc
        # | limit 20

        query_id = start_query_response['queryId']

        response = None

        while response is None or response['status'] == 'Running':
            time.sleep(1)
            response = self.client.get_query_results(
                queryId=query_id
            )
        return response
