from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
  coalesce, 
  col, 
  when, 
  sum as spark_sum, 
  broadcast, 
  avg, 
  dense_rank, 
  max as spark_max, 
  to_date, 
  lit, 
  greatest,
  round as spark_round
  )

def deduplicate_events(df: DataFrame) -> DataFrame:
  """
  While the SLA with the vendor guarantees At-least-once delivery, this function
  removes any duplicate messages received

  Args:
    df (DataFrame): Dataframe with same schema after reading from engagement json
  
  Returns:
    deduplicated_df: Dataframe with any exact duplicate rows removed
  """
  deduplicated_df = df.dropDuplicates(["userId", "campaign", "action", "eventTimestamp"])

  return deduplicated_df


def remove_failed_deliveries(df: DataFrame) -> DataFrame:
  """
  Failed message deliveries are irrelevant to our report metrics, this function
  removes them to reduce downstream processing requirements.

  Args:
    df (DataFrame): Dataframe with same schema after reading from engagement json

  Returns:
    cleaned_df (DataFrame): Dataframe with "DELIVERY_FAILED" events removed

  """
  cleaned_df = df.filter(col("action") != "DELIVERY_FAILED")

  return cleaned_df


def count_opens_per_user_and_campaign(df: DataFrame) -> DataFrame:
  """
  Counts the number of steps completed per campaign and user. A completed step
  is "MESSAGE_OPENED" action.
  Also gets the latest event date of delivered/opened message per campaign and user.

  Args:
    df (DataFrame): Dataframe with same schema after reading from engagement json,
      duplicate and failed events should be removed first using deduplicate_events()
      and remove_failed_deliveries()
  
  Returns:
    open_count_df (DataFrame):
      cols:
        - userId (String): Unique user identifier
        - campaign (String): Unique campaign ID
        - open_count (Integer): Total number of "MESSAGE_OPEN" per campaign/user in today's file
        - last_event_date (Date): Latest 
  """
  open_count_df = df.groupBy("userId", "campaign").agg(
    spark_sum(when(col("action") == "MESSAGE_OPENED", 1).otherwise(0)).alias("open_count"),
    to_date(spark_max("eventTimestamp")).alias("last_event_date")
  )

  return open_count_df


def calculate_user_campaign_completion(
  user_campaign_steps_df: DataFrame
  ) -> DataFrame:
  """
  Calculates the percentage of steps completed per campaign and user, referencing the
  number of steps a campaign involved at the time of the user's last message delivered/recieved event

  Args:
    user_campaign_steps_df (DataFrame): Dataframe showing users total open events and number of
      steps involved in a campaign per user/campaign

  Returns:
    percent_completion_df: Dataframe with "percentage_completion" column, which shows 
      the percentage of steps a user completed per campaign. Also includes "campaign_id"
      and "campaign_name" columns, which are required for downstream processing.
  """
  percent_completion_df = (
    user_campaign_steps_df
    .withColumn("percent_completion", col("open_count") / col("number_of_steps"))
    )
  
  return percent_completion_df


def calculate_campaign_average_completion(user_percent_completion_df: DataFrame) -> DataFrame:
  """
  Creates the final dataframe for the Current Campaign Engagement Report.

  Args:
    user_percent_completion_df (DataFrame): Dataframe with percentage of steps completed per user
    and campaign. Created by calculate_user_campaign_completion()
  
  Returns:
    rank_average_campaign_completion_df (DataFrame):
      cols:
        - campaign_name: Name of the campaign
        - average_percent_completion: Average percentage of steps completed across all users who received messages
        - rank: The ranking of campaigns based on average percent of steps completed
            Highest performing campaign is rank 1, ties recieve same rank.
  """
  averaged_df = (
    user_percent_completion_df.groupBy(col("campaign_name"), col("campaign_id"))
      .agg(avg("percent_completion")
      .alias("average_percent_completion"))
  )

  window_spec = Window.orderBy(col("average_percent_completion").desc())

  rank_average_campaign_completion_df = (
    averaged_df
    .withColumn("rank", dense_rank().over(window_spec))
    .select(
      "campaign_name", 
      spark_round(col("average_percent_completion"), 2).alias("average_percent_completion"), 
      "rank"
      )
    .orderBy("rank", "campaign_name")
    )

  return rank_average_campaign_completion_df


def update_user_state(yesterday_state_df: DataFrame, today_opens_df: DataFrame) -> DataFrame:
  """
  For each user and campaign, we need to keep track of culmulative open events across the daily engagement files,
  as well as the lastest message delivered/opened event. This is important to help us assertain the percentage of steps
  a user completed at the time of the activity (i.e if a user completed a campaign but then new steps were added at a later
  date, we'd still want to count them as having completed it).

  This function updates the user state dataframe with any new actions contained within the new daily data

  Args:
    yesterday_state_df (DataFrame): Dataframe containing previous user engagement state (culmative open_count and last_event_date per campaign/user)
    today_opens_df (DataFrame): Dataframe containing today's new open_event count and last_event_dates per campaign/user

  Returns:
    updated_user_state_df (DataFrame):
      cols:
        - userId (String): Unique user identifier
        - campaign (String): Unique campaign identifier
        - open_count (Long): Culmative number of messages opened across all engagement data files
        - last_event_date (Date): Latest message delivered or opened event
  """
  updated_user_state_df = (
    yesterday_state_df.join(today_opens_df, ["userId", "campaign"], "fullouter")
    .select(
      coalesce(yesterday_state_df["userId"], today_opens_df["userId"]).alias("userId"),
      coalesce(yesterday_state_df["campaign"], today_opens_df["campaign"]).alias("campaign"),
      (coalesce(yesterday_state_df["open_count"], lit(0)) + coalesce(today_opens_df["open_count"], lit(0))).alias("open_count"),
      greatest(coalesce(yesterday_state_df["last_event_date"], lit(date(1900, 1, 1))), coalesce(today_opens_df["last_event_date"], lit(date(1900, 1, 1)))).alias("last_event_date")
    )
    )

  return updated_user_state_df


def join_user_state_with_campaign_snapshot(user_state_df: DataFrame, campaigns_snapshot_df: DataFrame) -> DataFrame:
  """
  Joins the user state dataframe with the campaign snapshot dataframe to bring in
  the number of steps a campaign had at the time of the user's last event, enabling
  accurate percent completion calculation.

  Args:
    user_state_df (DataFrame): Dataframe containing cumulative user engagement state
    campaigns_snapshot_df (DataFrame): Dataframe containing historical campaign snapshots

  Returns:
    user_campaign_steps_df (DataFrame):
      cols:
        - userId (String): Unique user identifier
        - campaign_id (String): Unique campaign identifier
        - campaign_name (String): Name of the campaign
        - open_count (Long): Cumulative number of messages opened
        - number_of_steps (Integer): Number of steps in the campaign at time of user's last event
  """
  user_campaign_steps_df = (
    user_state_df
    .join(
      broadcast(campaigns_snapshot_df),
      (user_state_df.campaign == campaigns_snapshot_df.campaign_id) &
      (user_state_df.last_event_date == campaigns_snapshot_df.snapshot_date)
      )
    .select(
      user_state_df.userId,
      campaigns_snapshot_df.campaign_id,
      campaigns_snapshot_df.campaign_name,
      user_state_df.open_count,
      campaigns_snapshot_df.number_of_steps
    )
  )

  return user_campaign_steps_df