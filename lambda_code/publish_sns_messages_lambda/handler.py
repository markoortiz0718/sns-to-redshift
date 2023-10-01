import json
import random
import string
from typing import Any

import boto3

client = boto3.client("sns")

SECTORS = [
    "Agriculture",
    "Chemical",
    "Construction",
    "Education",
    "Financial services",
    "Professional services",
    "Food, drink, tobacco",
    "Forestry",
    "Health services",
    "Tourism",
    "Mining",
    "Media",
    "Oil and gas",
    "Telecommunications",
    "Shipping",
    "Textile",
    "Transportation",
    "Utilities",
]
CONSOLES = ["Mobile", "PC", "PlayStation", "Switch", "XBox"]
GENRES = ["Action", "Puzzle", "Roleplaying", "Shooter", "Sports"]


def publish_sns_messages(sns_topic_arn: str, messages: list[str, Any]):
    failed_messages = []
    for mini_batch in [
        messages[idx : idx + 10] for idx in range(0, len(messages), 10)
    ]:  # publish_batch() supports max of 10 elements
        response = client.publish_batch(  # I do see duplicate messages
            TopicArn=sns_topic_arn,  # from SNS's at least once delivery
            PublishBatchRequestEntries=[
                {"Id": str(i), "Message": json.dumps(message)}
                for i, message in enumerate(mini_batch)
            ],
        )
        failed = response["Failed"]
        failed_messages.extend(failed)
        if failed_messages:
            print(failed)
    if failed_messages:
        raise Exception(failed_messages)


def lambda_handler(event, context) -> None:
    sns_topic_name = event["SNS_TOPIC_NAME"]
    sns_num_messages = event["SNS_NUM_MESSAGES"]
    sns_topic_arn = event["sns_topic_arn"]
    if sns_topic_name == "stock-data-topic":
        messages = [
            {
                "TICKER_SYMBOL": "".join(
                    random.choice(string.ascii_uppercase) for _ in range(4)
                ),
                "SECTOR": random.choice(SECTORS),
                # Firehose transform Lambda will convert this str to float
                "CHANGE": str(round((random.random() - 0.5) * 10, 2)),
                # Firehose transform Lambda will convert this str to int
                "PRICE": str(random.randrange(-100, 100)),
            }
            for _ in range(sns_num_messages)
        ]
    elif sns_topic_name == "game-data-topic":
        messages = [
            {
                "CONSOLE": random.choice(CONSOLES),
                "GENRE": random.choice(GENRES),
                "YEAR": random.randint(2000, 2023),
                "SALES": random.randint(0, 100_000_000),
            }
            for _ in range(sns_num_messages)
        ]
    else:
        raise ValueError(f"Unexpected topic name: {sns_topic_name}")
    publish_sns_messages(sns_topic_arn=sns_topic_arn, messages=messages)
