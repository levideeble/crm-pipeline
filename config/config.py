class Config:
  """
  Holds config settings for the CRM pipeline.

  Attributes:
    gcs_bucket_name (str): GCS bucket name
    date (str): Date to process in YYYYMMDD format
    input_path (str): GCS path for incoming supplier files
    internal_path (str): GCS path for internal state/snapshots
    output_path (str): GCS path for output reports
    campaigns_file_pattern (str): Wildcard file pattern for matching campaigns input file
    engagement_file_pattern (str): Wildcard file pattern for matching engagement input file
  """
  
  def __init__(self, gcs_bucket_name:str, date:str, base_path:str = None):
    """
    Initialises Config with GCS bucket name and processing date.

    Args:
      gcs_bucket_name (str): GCS bucket name
      date (str): Date to process in YYYYMMDD format
      base_path (str, optional): Override for the base storage path. Defaults to None,
        in which case the GCS bucket path is used. Intended for local/integration testing,
        where a local file path can be substituted instead.
    """
    self.gcs_bucket_name = gcs_bucket_name
    self.date = date

    base = base_path or f"gs://{gcs_bucket_name}"
    
    self.input_path = f"{base}/input/daily_files/"
    self.internal_path = f"{base}/internal/"
    self.output_path = f"{base}/output/reports/"

    # Filename patters - vendor's actual naming conventions are unconfirmed, these
    # are assumptions that can be easily swapped out
    self.campaigns_file_pattern = f"*campaign*{date}*.json"
    self.engagement_file_pattern = f"*engagement*{date}*.json"