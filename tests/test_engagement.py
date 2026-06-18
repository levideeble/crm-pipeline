from pyspark.testing import assertDataFrameEqual
from pyspark.sql.functions import col
from pyspark.sql.types import *
from datetime import date
from transformations.engagement import (
  deduplicate_events, 
  remove_failed_deliveries,
  count_opens_per_user_and_campaign,
  calculate_user_campaign_completion,
  calculate_campaign_average_completion,
  update_user_state,
  join_user_state_with_campaign_snapshot
)

class TestDeduplicateEvents:
  
  def test_removed_exact_duplicate_events(self, spark):
    data = [
      {
        "userId": "user1",
        "campaign": "6fg7e8",
        "eventTimestamp": "2023-07-11T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      },
      {
        "userId": "user1",
        "campaign": "6fg7e8",
        "eventTimestamp": "2023-07-11T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      }
    ]

    df = spark.createDataFrame(data)

    result = deduplicate_events(df)

    assert result.count() == 1


class TestRemoveFailedDeliveries:
  
  def test_failed_delivery_message_removed(self, spark):
    input_data = [
      {
        "userId": "user1",
        "campaign": "6fg7e8",
        "eventTimestamp": "2023-07-11T09:08:19.9947",
        "action": "DELIVERY_FAILED"
      },
      {
        "userId": "user1",
        "campaign": "6fg7e8",
        "eventTimestamp": "2023-07-12T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      }
    ]

    input_df = spark.createDataFrame(input_data)

    expected_data = [
      {
        "userId": "user1",
        "campaign": "6fg7e8",
        "eventTimestamp": "2023-07-12T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      }
    ]

    expected_df = spark.createDataFrame(expected_data)

    actual_df = remove_failed_deliveries(input_df)

    assertDataFrameEqual(actual_df, expected_df)


class TestCountOpensPerUserAndCampaign:
  
  def test_counts_opens_correctly_for_user_with_opens(self, spark):
    input_data = [
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-11T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      },
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-12T09:08:19.9947",
        "action": "MESSAGE_OPENED"
      },
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-13T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      },
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-14T09:08:19.9947",
        "action": "MESSAGE_OPENED"
      }
    ]

    input_df = spark.createDataFrame(input_data)

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 2,
          "last_event_date": date(2023, 7, 14)
        }
      ]
    )

    actual_df = count_opens_per_user_and_campaign(input_df)

    assertDataFrameEqual(actual_df, expected_df, ignoreColumnOrder=True)


  def test_user_with_zero_opens_has_count_zero(self, spark):
    input_data = [
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-11T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      },
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-13T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      }
    ]

    input_df = spark.createDataFrame(input_data)

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 0,
          "last_event_date": date(2023, 7, 13)
        }
      ]
    )

    actual_df = count_opens_per_user_and_campaign(input_df)

    assertDataFrameEqual(actual_df, expected_df, ignoreColumnOrder=True)


  def test_separates_counts_by_campaign_and_user(self, spark):
    input_data = [
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-11T09:08:19.9947",
        "action": "MESSAGE_OPENED"
      },
      {
        "userId": "user1",
        "campaign": "campaign2",
        "eventTimestamp": "2023-07-12T09:08:19.9947",
        "action": "MESSAGE_OPENED"
      },
      {
        "userId": "user2",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-13T09:08:19.9947",
        "action": "MESSAGE_OPENED"
      },
      {
        "userId": "user2",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-14T09:08:19.9947",
        "action": "MESSAGE_OPENED"
      }
    ]

    input_df = spark.createDataFrame(input_data)

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 11)
        },
        {
          "userId": "user1", 
          "campaign": "campaign2", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 12)
        },
        {
          "userId": "user2", 
          "campaign": "campaign1", 
          "open_count": 2,
          "last_event_date": date(2023, 7, 14)
        }
      ]
    )

    actual_df = count_opens_per_user_and_campaign(input_df)

    assertDataFrameEqual(actual_df, expected_df, ignoreColumnOrder=True)


  def test_latest_event_date_correct(self, spark):
    input_data = [
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-11T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      },
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-15T09:08:19.9947",
        "action": "MESSAGE_DELIVERED"
      },
      {
        "userId": "user1",
        "campaign": "campaign1",
        "eventTimestamp": "2023-07-12T09:08:19.9947",
        "action": "MESSAGE_OPENED"
      }
    ]

    input_df = spark.createDataFrame(input_data)

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 15)
        }
      ]
    )

    actual_df = count_opens_per_user_and_campaign(input_df)

    assertDataFrameEqual(actual_df, expected_df, ignoreColumnOrder=True)


class TestCalculateUserCampaignCompletion:
  
  def test_basic_percentage_completion_logic(self, spark):
    user_campaign_steps_df = spark.createDataFrame(
      [
        {
          "userId": "user1",
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "open_count": 3,
          "number_of_steps": 4
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign_id": "campaign_1_id", 
          "campaign_name": "campaign_1_name", 
          "open_count": 3, 
          "number_of_steps": 4, 
          "percent_completion": 0.75
        }
      ]
    )

    actual_df = calculate_user_campaign_completion(user_campaign_steps_df)

    assertDataFrameEqual(actual_df, expected_df, ignoreColumnOrder=True)


  def test_zero_open_user_campaign(self, spark):
    user_campaign_steps_df = spark.createDataFrame(
      [
        {
          "userId": "user1",
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "open_count": 0,
          "number_of_steps": 4
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign_id": "campaign_1_id", 
          "campaign_name": "campaign_1_name", 
          "open_count": 0, 
          "number_of_steps": 4, 
          "percent_completion": 0.0
        }
      ]
    )

    actual_df = calculate_user_campaign_completion(user_campaign_steps_df)

    assertDataFrameEqual(actual_df, expected_df, ignoreColumnOrder=True)

  
  def test_user_completed_campaign(self, spark):
    user_campaign_steps_df = spark.createDataFrame(
      [
        {
          "userId": "user1",
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "open_count": 4,
          "number_of_steps": 4
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign_id": "campaign_1_id", 
          "campaign_name": "campaign_1_name", 
          "open_count": 4, 
          "number_of_steps": 4, 
          "percent_completion": 1.0
        }
      ]
    )

    actual_df = calculate_user_campaign_completion(user_campaign_steps_df)

    assertDataFrameEqual(actual_df, expected_df, ignoreColumnOrder=True)


class TestCalculateCampaignAverageCompletion:

  def test_campaign_average_no_zeros(self, spark):
    user_percent_completion_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign_id": "campaign_1_id", 
          "campaign_name": "campaign_1_name", 
          "open_count": 2, 
          "number_of_steps": 4, 
          "percent_completion": 0.50
        },
        {
          "userId": "user2", 
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name", 
          "open_count": 4,
          "number_of_steps": 4, 
          "percent_completion": 1.0
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        ("campaign_1_name", 0.75, 1)
      ],
      schema = StructType(
        [
        StructField("campaign_name", StringType()),
        StructField("average_percent_completion", DoubleType()),
        StructField("rank", IntegerType())
        ]
        )
    )

    actual_df = calculate_campaign_average_completion(user_percent_completion_df)

    assertDataFrameEqual(actual_df, expected_df)


  def test_campaign_average_with_zeros(self, spark):
    user_percent_completion_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign_id": "campaign_1_id", 
          "campaign_name": "campaign_1_name", 
          "open_count": 2, 
          "number_of_steps": 4, 
          "percent_completion": 0.50
        },
        {
          "userId": "user2", 
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name", 
          "open_count": 0,
          "number_of_steps": 4, 
          "percent_completion": 0.0
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        ("campaign_1_name", 0.25, 1)
      ],
      schema = StructType(
        [
        StructField("campaign_name", StringType()),
        StructField("average_percent_completion", DoubleType()),
        StructField("rank", IntegerType())
        ]
        )
    )

    actual_df = calculate_campaign_average_completion(user_percent_completion_df)

    assertDataFrameEqual(actual_df, expected_df)


  def test_campaign_average_basic_rank(self, spark):
    user_percent_completion_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign_id": "campaign_1_id", 
          "campaign_name": "campaign_1_name", 
          "open_count": 0, 
          "number_of_steps": 4, 
          "percent_completion": 0.0
        },
        {
          "userId": "user1", 
          "campaign_id": "campaign_2_id",
          "campaign_name": "campaign_2_name", 
          "open_count": 4,
          "number_of_steps": 4, 
          "percent_completion": 1.0
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        ("campaign_2_name", 1.0, 1),
        ("campaign_1_name", 0.0, 2)

      ],
      schema = StructType(
        [
        StructField("campaign_name", StringType()),
        StructField("average_percent_completion", DoubleType()),
        StructField("rank", IntegerType())
        ]
        )
    )

    actual_df = calculate_campaign_average_completion(user_percent_completion_df)

    assertDataFrameEqual(actual_df, expected_df)


  def test_campaign_average_tied_rank(self, spark):
    user_percent_completion_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign_id": "campaign_1_id", 
          "campaign_name": "campaign_1_name", 
          "open_count": 0, 
          "number_of_steps": 4, 
          "percent_completion": 0.0
        },
        {
          "userId": "user1", 
          "campaign_id": "campaign_2_id",
          "campaign_name": "campaign_2_name", 
          "open_count": 4,
          "number_of_steps": 4, 
          "percent_completion": 1.0
        },
        {
          "userId": "user1", 
          "campaign_id": "campaign_3_id",
          "campaign_name": "campaign_3_name", 
          "open_count": 4,
          "number_of_steps": 4, 
          "percent_completion": 1.0
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        ("campaign_2_name", 1.0, 1),
        ("campaign_3_name", 1.0, 1),
        ("campaign_1_name", 0.0, 2)

      ],
      schema = StructType(
        [
        StructField("campaign_name", StringType()),
        StructField("average_percent_completion", DoubleType()),
        StructField("rank", IntegerType())
        ]
        )
    )

    actual_df = calculate_campaign_average_completion(user_percent_completion_df)

    assertDataFrameEqual(actual_df, expected_df, checkRowOrder=True)


  def test_average_percent_completion_rounded_to_2dp(self, spark):
    user_percent_completion_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign_id": "campaign_1_id", 
          "campaign_name": "campaign_1_name", 
          "open_count": 2, 
          "number_of_steps": 3, 
          "percent_completion": 0.66666
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        ("campaign_1_name", 0.67, 1)
      ],
      schema = StructType(
        [
        StructField("campaign_name", StringType()),
        StructField("average_percent_completion", DoubleType()),
        StructField("rank", IntegerType())
        ]
        )
    )

    actual_df = calculate_campaign_average_completion(user_percent_completion_df)

    assertDataFrameEqual(actual_df, expected_df)



class TestUpdateUserState:
  def test_new_user_state_added_and_inactive_user_unchanged(self, spark):
    yesterday_state_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 11)
        }
      ]
    )

    today_opens_df = spark.createDataFrame(
      [
        {
          "userId": "user2", 
          "campaign": "campaign1", 
          "open_count": 0,
          "last_event_date": date(2023, 7, 12)
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 11)
        },
        {
          "userId": "user2", 
          "campaign": "campaign1", 
          "open_count": 0,
          "last_event_date": date(2023, 7, 12)
        }
      ],
      schema = StructType(
        [
        StructField("userId", StringType()),
        StructField("campaign", StringType()),
        StructField("open_count", LongType()),
        StructField("last_event_date", DateType())
        ]
        )
    )

    actual_df = update_user_state(yesterday_state_df, today_opens_df)

    assertDataFrameEqual(actual_df, expected_df)

  
  def test_active_existing_user(self, spark):
    yesterday_state_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 11)
        }
      ]
    )

    today_opens_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 2,
          "last_event_date": date(2023, 7, 12)
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 3,
          "last_event_date": date(2023, 7, 12)
        }
      ],
      schema = StructType(
        [
        StructField("userId", StringType()),
        StructField("campaign", StringType()),
        StructField("open_count", LongType()),
        StructField("last_event_date", DateType())
        ]
        )
    )

    actual_df = update_user_state(yesterday_state_df, today_opens_df)

    assertDataFrameEqual(actual_df, expected_df)


  def test_incorrect_order_last_event_date(self, spark):
    yesterday_state_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 12)
        }
      ]
    )

    today_opens_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 2,
          "last_event_date": date(2023, 7, 11)
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign1", 
          "open_count": 3,
          "last_event_date": date(2023, 7, 12)
        }
      ],
      schema = StructType(
        [
        StructField("userId", StringType()),
        StructField("campaign", StringType()),
        StructField("open_count", LongType()),
        StructField("last_event_date", DateType())
        ]
        )
    )

    actual_df = update_user_state(yesterday_state_df, today_opens_df)

    assertDataFrameEqual(actual_df, expected_df)


class TestJoinUserStateWithCampaignSnapshot:
  def test_basic_user_state_and_campaign_snapshot_match(self, spark):
    user_state_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign_1_id", 
          "open_count": 3,
          "last_event_date": date(2023, 7, 12)
        }
      ]
    )

    campaign_snapshot_df = spark.createDataFrame(
      [
        {
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "number_of_steps": 4,
          "snapshot_date": date(2023, 7, 12)
        },
        {
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "number_of_steps": 5,
          "snapshot_date": date(2023, 7, 13)
        }
      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1",
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "open_count": 3,
          "number_of_steps": 4
        }
      ],
      schema = StructType(
        [
        StructField("userId", StringType()),
        StructField("campaign_id", StringType()),
        StructField("campaign_name", StringType()),
        StructField("open_count", LongType()),
        StructField("number_of_steps", LongType())
        ]
        )
    )

    actual_df = join_user_state_with_campaign_snapshot(user_state_df, campaign_snapshot_df)

    assertDataFrameEqual(actual_df, expected_df)


  def test_campaign_number_of_steps_changes(self, spark):
    user_state_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign_1_id", 
          "open_count": 3,
          "last_event_date": date(2023, 7, 12)
        },
        {
          "userId": "user2", 
          "campaign": "campaign_1_id", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 13)
        }
      ]
    )

    campaign_snapshot_df = spark.createDataFrame(
      [
        {
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "number_of_steps": 4,
          "snapshot_date": date(2023, 7, 12)
        },
        {
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "number_of_steps": 5,
          "snapshot_date": date(2023, 7, 13)
        }

      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1",
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "open_count": 3,
          "number_of_steps": 4
        },
        {
          "userId": "user2",
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "open_count": 1,
          "number_of_steps": 5
        }
      ],
      schema = StructType(
        [
        StructField("userId", StringType()),
        StructField("campaign_id", StringType()),
        StructField("campaign_name", StringType()),
        StructField("open_count", LongType()),
        StructField("number_of_steps", LongType())
        ]
        )
    )

    actual_df = join_user_state_with_campaign_snapshot(user_state_df, campaign_snapshot_df)

    assertDataFrameEqual(actual_df, expected_df)


  def test_multiple_campaigns_correctly_joined(self, spark):
    user_state_df = spark.createDataFrame(
      [
        {
          "userId": "user1", 
          "campaign": "campaign_1_id", 
          "open_count": 3,
          "last_event_date": date(2023, 7, 12)
        },
        {
          "userId": "user1", 
          "campaign": "campaign_2_id", 
          "open_count": 1,
          "last_event_date": date(2023, 7, 12)
        }
      ]
    )

    campaign_snapshot_df = spark.createDataFrame(
      [
        {
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "number_of_steps": 4,
          "snapshot_date": date(2023, 7, 12)
        },
        {
          "campaign_id": "campaign_2_id",
          "campaign_name": "campaign_2_name",
          "number_of_steps": 2,
          "snapshot_date": date(2023, 7, 12)
        }

      ]
    )

    expected_df = spark.createDataFrame(
      [
        {
          "userId": "user1",
          "campaign_id": "campaign_1_id",
          "campaign_name": "campaign_1_name",
          "open_count": 3,
          "number_of_steps": 4
        },
        {
          "userId": "user1",
          "campaign_id": "campaign_2_id",
          "campaign_name": "campaign_2_name",
          "open_count": 1,
          "number_of_steps": 2
        }
      ],
      schema = StructType(
        [
        StructField("userId", StringType()),
        StructField("campaign_id", StringType()),
        StructField("campaign_name", StringType()),
        StructField("open_count", LongType()),
        StructField("number_of_steps", LongType())
        ]
        )
    )

    actual_df = join_user_state_with_campaign_snapshot(user_state_df, campaign_snapshot_df)

    assertDataFrameEqual(actual_df, expected_df)
