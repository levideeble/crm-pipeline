import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date
from pyspark.errors import AnalysisException
from config.config import Config

from transformations.campaigns import process_campaigns
from transformations.engagement import (
  deduplicate_events,
  remove_failed_deliveries,
  count_opens_per_user_and_campaign,
  update_user_state,
  join_user_state_with_campaign_snapshot,
  calculate_user_campaign_completion,
  calculate_campaign_average_completion
)


def main(bucket_name: str, date: str, base_path: str = None):
  """
  Orchestrates the daily CRM campaign pipeline: 
    reads campaign and engagement data, updates internal state, and writes the Campaign Overview
    and Current Campaign Engagement reports.

  Args:
    bucket_name (str): GCS bucket name
    date (str): Date to process, format YYYYMMDD
  """

  spark = SparkSession.builder.appName("Daily CRM Campaign Pipeline").getOrCreate()
  config = Config(bucket_name, date, base_path=base_path)


  # Read the raw campaigns json file from GCS for the date the pipeline is being run for
  campaigns_path = config.input_path + config.campaigns_file_pattern
  raw_campaigns_df = spark.read.option("multiline", "true").json(campaigns_path)

  # Build the processed_campaigns dataframe, which is in the format required for campaigns report.
  # This will also be used to help build engagement report.
  processed_campaigns_df = process_campaigns(raw_campaigns_df)

  # Adds the snapshot_date column, which is required to keep track of changes to number_of_steps in campaigns
  campaign_snapshot_df = processed_campaigns_df.withColumn(
    "snapshot_date", to_date(lit(date), "yyyyMMdd")
  )

  # Read existing snapshot file from internal GCS folder. Handles error if doesn't exist yet (i.e on day one)
  campaign_snapshot_path = config.internal_path + "campaign_snapshot/"

  try:
    existing_snapshot_df = spark.read.parquet(campaign_snapshot_path)
  except AnalysisException:
    existing_snapshot_df = None

  # Combine the existing snapshot dataframe (if exists) with the new snapshot dataframe, and write back to internal
  if existing_snapshot_df is not None:
    combined_snapshot_df = existing_snapshot_df.union(campaign_snapshot_df)
  else:
    combined_snapshot_df = campaign_snapshot_df

  combined_snapshot_df = combined_snapshot_df.cache()
  combined_snapshot_df.count()

  combined_snapshot_df.write.mode("overwrite").parquet(campaign_snapshot_path)

  # Write the campaign overview CSV to the output location within a dated sub-folder
  campaign_overview_report_path = config.output_path + f"campaign_overview/campaign_overview_{date}/"
  processed_campaigns_df.write.mode("overwrite").csv(campaign_overview_report_path, header=True)

  # Read and clean the raw engagement json file from GCS for the date the pipeline is being run for
  engagement_path = config.input_path + config.engagement_file_pattern
  raw_engagement_df = spark.read.option("multiline", "true").json(engagement_path)

  deduplicated_engagement_df = deduplicate_events(raw_engagement_df)
  cleaned_engagement_df = remove_failed_deliveries(deduplicated_engagement_df)

  # Build dataframe containing the open events per user/campaign for date pipeline is being run for
  today_opens_df = count_opens_per_user_and_campaign(cleaned_engagement_df)

  
  # Read existing user state file from internal GCS folder. Handles error if doesn't exist yet (i.e on day one)
  user_state_path = config.internal_path + "user_state/"

  try:
    existing_user_state_df = spark.read.parquet(user_state_path)
  except AnalysisException:
    existing_user_state_df = None
  
  # Update user state dataframe (if exists) using any new events, and write back to internal.
  # If there's not already a user state file, a new one is created
  if existing_user_state_df is not None:
    updated_user_state_df=update_user_state(
      yesterday_state_df=existing_user_state_df,
      today_opens_df=today_opens_df
      )
  else:
    updated_user_state_df = today_opens_df

  updated_user_state_df = updated_user_state_df.cache()
  updated_user_state_df.count()
  
  updated_user_state_df.write.mode("overwrite").parquet(user_state_path)


  # Join the updated user state dataframe with the campaign snapshot history dataframe
  user_campaign_steps_df = join_user_state_with_campaign_snapshot(
    user_state_df=updated_user_state_df,
    campaigns_snapshot_df=combined_snapshot_df
  )

  # Calculate the percentage of campaign steps that users have completed, and write the final
  # Current Campaign Engagement Report to the GCS output location
  user_completion_df = calculate_user_campaign_completion(user_campaign_steps_df)
  campaign_engagement_df = calculate_campaign_average_completion(user_completion_df)

  current_campaign_engagement_report_path = (
    config.output_path + f"current_campaign_engagement_report/current_campaign_engagement_report_{date}/"
  )
  campaign_engagement_df.write.mode("overwrite").csv(current_campaign_engagement_report_path, header=True)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--bucket", required=True, help="GCS bucket name")
  parser.add_argument("--date", required=True, help="Date to process, format YYYYMMDD")
  args = parser.parse_args()
  main(args.bucket, args.date)
