import pytest
import json
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
  """Creates a Spark session for testing."""
  return SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()

@pytest.fixture()
def mock_campaigns_df(spark):
  """Creates a mock campaigns dataframe, mimicking spark.read.json() output."""
  mock_campaigns_json = [
    {
        "id": "6fg7e8",
        "details": {
            "name": "summer_romance_binge",
            "schedule": [
                "2023-07-21",
                "2023-07-31"
            ]
        },
        "steps": [
            { "templateId": "f0993"},
            { "templateId": "857a8"},
            { "templateId": "36b43"},
            { "templateId": "62335"}
        ]
    },
    {
        "id": "cb571",
        "details": {
            "name": "win_back",
            "schedule": [
                "2023-07-01",
                "2023-07-25"
            ]
        },
        "steps": [
            { "templateId": "f0993"},
            { "templateId": "857a8"},
            { "templateId": "62335"}
        ]
    }
  ]

  with open("/tmp/mock_campaigns.json", "w") as f:
    for campaign in mock_campaigns_json:
      f.write(json.dumps(campaign) + "\n")

  mock_campaigns_df = spark.read.json("/tmp/mock_campaigns.json")

  return mock_campaigns_df


@pytest.fixture()
def mock_engagement_df(spark):
  """Creates a mock engagement dataframe, mimicking spark.read.json() output."""
  mock_engagement_json = [
    {
      "userId": "user1",
      "campaign": "6fg7e8",
      "eventTimestamp": "2023-07-11T09:08:19.9947",
      "action": "MESSAGE_DELIVERED"
    },
    {
      "userId": "user1",
      "campaign": "6fg7e8",
      "eventTimestamp": "2023-07-13T09:08:19.9947",
      "action": "MESSAGE_OPENED"
    },
    {
      "userId": "user1",
      "campaign": "6fg7e8",
      "eventTimestamp": "2023-07-16T09:08:19.9947",
      "action": "DELIVERY_FAILED"
    },
    {
      "userId": "user1",
      "campaign": "6fg7e8",
      "eventTimestamp": "2023-07-17T09:08:19.9947",
      "action": "MESSAGE_DELIVERED"
    },
    {
      "userId": "user2",
      "campaign": "cb571",
      "eventTimestamp": "2023-07-18T09:08:19.9947",
      "action": "MESSAGE_DELIVERED"
    },
    {
      "userId": "user2",
      "campaign": "cb571",
      "eventTimestamp": "2023-07-19T09:08:19.9947",
      "action": "MESSAGE_OPENED"
    },
    {
      "userId": "user2",
      "campaign": "cb571",
      "eventTimestamp": "2023-07-20T09:08:19.9947",
      "action": "MESSAGE_DELIVERED"
    },
    {
      "userId": "user2",
      "campaign": "cb571",
      "eventTimestamp": "2023-07-21T09:08:19.9947",
      "action": "MESSAGE_OPENED"
    },
    {
      "userId": "user2",
      "campaign": "cb571",
      "eventTimestamp": "2023-07-22T09:08:19.9947",
      "action": "MESSAGE_DELIVERED"
    },
    {
      "userId": "user2",
      "campaign": "cb571",
      "eventTimestamp": "2023-07-23T09:08:19.9947",
      "action": "MESSAGE_OPENED"
    }
  ]

  with open("/tmp/mock_engagement.json", "w") as f:
    for event in mock_engagement_json:
      f.write(json.dumps(event) + "\n")

  mock_engagement_df = spark.read.json("/tmp/mock_engagement.json")

  return mock_engagement_df
