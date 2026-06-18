from pyspark.sql import DataFrame
from pyspark.sql.functions import col, size, to_date

def process_campaigns(raw_df:DataFrame) -> DataFrame:
  """
  Takes the raw campaigns dataframe that is created when the campaigns json is
  read in using spark.read.json(), in its original format, and outputs a processed
  dataframe in the required format for the Campaign Overview report 
  (incl. number_of_steps calculation)

  Args:
    raw_df (DataFrame): Dataframe with same schema after reading from campaigns json
  
  Returns:
    processed_df (DataFrame): 
      Processed dataframe with the following cols:
        * campaign_id (str): Unique campaign ID
        * campaign_name (str): The name of the campaign
        * number_of_steps (int): Count of number of actionable steps within the campaign
        * start_date (date): The date on which the campaign starts
        * end_date (date): The expiry date of the campaign
  """
  processed_df = raw_df.select(
    col("id").alias("campaign_id"),
    col("details.name").alias("campaign_name"),
    size(col("steps")).alias("number_of_steps"),
    to_date(col("details.schedule")[0]).alias("start_date"),
    to_date(col("details.schedule")[1]).alias("end_date")
  )
  return processed_df