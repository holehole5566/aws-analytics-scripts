# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with Amazon Kinesis to create and
manage streams.
"""
import boto3
import json
import logging
from botocore.exceptions import ClientError
from datetime import datetime
import random
import os
from dotenv import load_dotenv # type: ignore
load_dotenv()

logger = logging.getLogger(__name__)


# snippet-start:[python.example_code.kinesis.KinesisStream.class]
class KinesisStream:
    """Encapsulates a Kinesis stream."""

    def __init__(self, kinesis_client):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.name = None
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter("stream_exists")

    # snippet-end:[python.example_code.kinesis.KinesisStream.class]

    def _clear(self):
        """
        Clears property data of the stream object.
        """
        self.name = None
        self.details = None

    def arn(self):
        """
        Gets the Amazon Resource Name (ARN) of the stream.
        """
        return self.details["StreamARN"]

    # snippet-start:[python.example_code.kinesis.CreateStream]
    def create(self, name, wait_until_exists=True):
        """
        Creates a stream.

        :param name: The name of the stream.
        :param wait_until_exists: When True, waits until the service reports that
                                  the stream exists, then queries for its metadata.
        """
        try:
            self.kinesis_client.create_stream(StreamName=name, ShardCount=1, StreamModeDetails={
                'StreamMode': 'ON_DEMAND'
            })
            self.name = name
            logger.info("Created stream %s.", name)
            if wait_until_exists:
                logger.info("Waiting until exists.")
                self.stream_exists_waiter.wait(StreamName=name)
                self.describe(name)
        except ClientError:
            logger.exception("Couldn't create stream %s.", name)
            raise

    # snippet-end:[python.example_code.kinesis.CreateStream]

    # snippet-start:[python.example_code.kinesis.DescribeStream]
    def describe(self, name):
        """
        Gets metadata about a stream.

        :param name: The name of the stream.
        :return: Metadata about the stream.
        """
        try:
            response = self.kinesis_client.describe_stream(StreamName=name)
            self.name = name
            self.details = response["StreamDescription"]
            logger.info("Got stream %s.", name)
        except ClientError:
            logger.exception("Couldn't get %s.", name)
            raise
        else:
            return self.details

    # snippet-end:[python.example_code.kinesis.DescribeStream]

    # snippet-start:[python.example_code.kinesis.DeleteStream]
    def delete(self):
        """
        Deletes a stream.
        """
        try:
            self.kinesis_client.delete_stream(StreamName=self.name)
            self._clear()
            logger.info("Deleted stream %s.", self.name)
        except ClientError:
            logger.exception("Couldn't delete stream %s.", self.name)
            raise

    # snippet-end:[python.example_code.kinesis.DeleteStream]

    # snippet-start:[python.example_code.kinesis.PutRecord]
    def put_record(self, data, partition_key):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.name, Data=json.dumps(data), PartitionKey=partition_key
            )
            logger.debug("Put record in stream %s, shard: %s", self.name, response["ShardId"])
            return response
        except ClientError:
            logger.exception("Couldn't put record in stream %s.", self.name)
            raise

    # snippet-end:[python.example_code.kinesis.PutRecord]

    # snippet-start:[python.example_code.kinesis.GetRecords]
    def get_records(self, max_records):
        """
        Gets records from the stream. This function is a generator that first gets
        a shard iterator for the stream, then uses the shard iterator to get records
        in batches from the stream. Each batch of records is yielded back to the
        caller until the specified maximum number of records has been retrieved.

        :param max_records: The maximum number of records to retrieve.
        :return: Yields the current batch of retrieved records.
        """
        try:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=self.name,
                ShardId=self.details["Shards"][0]["ShardId"],
                ShardIteratorType="LATEST",
            )
            shard_iter = response["ShardIterator"]
            print(shard_iter)
            record_count = 0
            while record_count < max_records:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iter, Limit=10
                )
                shard_iter = response["NextShardIterator"]
                records = response["Records"]
                print(response)
                print("Got %s records.", len(records))
                record_count += len(records)
                yield records
        except ClientError:
            logger.exception("Couldn't get records from stream %s.", self.name)
            raise


# snippet-end:[python.example_code.kinesis.GetRecords]

import time
import signal
import sys
from datetime import timezone

TICKERS = ["EKS", "S3", "EC2"]
RECORDS_PER_SECOND = 10
PARTITION_COUNT = 10

def generate_record():
    """Generate a single record with realistic timestamp"""
    now = datetime.now(timezone.utc)
    return {
        "metadata": {
            "product": {
                "name": random.choice(TICKERS)
            }
        },
        "message": "ResponseComplete",
        "time": int(now.timestamp() * 1000),
        "severity": "Informational",
        "activity_name": "Update",
        "@timestamp": now.isoformat()
    }

def signal_handler(sig, frame):
    """Handle graceful shutdown"""
    print("\nShutting down gracefully...")
    sys.exit(0)

def main():
    """Main producer function"""
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Validate environment variables
    stream_name = os.getenv("KDS_NAME")
    if not stream_name:
        raise ValueError("KDS_NAME environment variable is required")
    
    region = os.getenv("AWS_REGION", "us-east-1")
    
    try:
        # Create Kinesis client and stream
        kinesis_client = boto3.client("kinesis", region_name=region)
        stream = KinesisStream(kinesis_client)
        stream.describe(stream_name)
        
        print(f"Connected to Kinesis stream: {stream_name}")
        print(f"Sending {RECORDS_PER_SECOND} records/second (Ctrl+C to stop)")
        
        counter = 0
        while True:
            try:
                # Generate and send record
                data = generate_record()
                partition_key = f"partition_key_{counter % PARTITION_COUNT}"
                
                stream.put_record(data, partition_key)
                counter += 1
                
                # Rate limiting
                time.sleep(1.0 / RECORDS_PER_SECOND)
                
                # Progress indicator
                if counter % 100 == 0:
                    print(f"Sent {counter} records")
                    
            except ClientError as e:
                print(f"Kinesis error: {e}")
                time.sleep(1)
            except Exception as e:
                print(f"Unexpected error: {e}")
                time.sleep(1)
                
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
