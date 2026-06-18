from datetime import date
from pyspark.sql.functions import col
from transformations.campaigns import process_campaigns

class TestProcessCampaigns:

  def test_process_campaigns_column_names(self, mock_campaigns_df):
    expected_columns = [
      "campaign_id", 
      "campaign_name", 
      "number_of_steps", 
      "start_date",
      "end_date"
      ]

    processed_df = process_campaigns(mock_campaigns_df)
    actual_columns = processed_df.columns

    assert expected_columns == actual_columns


  def test_process_campaigns_number_of_steps(self, mock_campaigns_df):
    expected_number_of_steps = [
      4,
      3
    ]

    processed_df = process_campaigns(mock_campaigns_df)
    actual_number_of_steps = [
      row["number_of_steps"] for row in processed_df.select(col("number_of_steps")).collect()
    ]
    
    assert expected_number_of_steps == actual_number_of_steps


  def test_process_campaigns_start_dates(self, mock_campaigns_df):
    expected_start_dates = [
      date(2023, 7, 21),
      date(2023, 7, 1)
    ]

    processed_df = process_campaigns(mock_campaigns_df)
    actual_start_dates = [
      row["start_date"] for row in processed_df.select(col("start_date")).collect()
    ]
    
    assert expected_start_dates == actual_start_dates


  def test_process_campaigns_end_dates(self, mock_campaigns_df):
    expected_end_dates = [
      date(2023, 7, 31),
      date(2023, 7, 25)
    ]

    processed_df = process_campaigns(mock_campaigns_df)
    actual_end_dates = [
      row["end_date"] for row in processed_df.select(col("end_date")).collect()
    ]

    assert expected_end_dates == actual_end_dates