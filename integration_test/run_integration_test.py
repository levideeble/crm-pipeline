import os
import sys
import shutil
import glob
import pandas as pd

# Add the project root (parent of this file's directory) to the path,
# so this script can be run directly (e.g. `python integration_test/run_integration_test.py`)
# without needing the package to be separately installed.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.daily_campaign_job import main

TEST_DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data")


def clean_internal_state():
    """
    Removes any existing internal state (campaign snapshots, user state)
    from a previous test run, ensuring the integration test starts from
    a clean slate. This is required because the pipeline assumes each date is
    processed exactly once.
    Rerunning without clearing internal state
    would incorrectly double-count engagement data (see README, known
    limitations)
    """
    internal_path = os.path.join(TEST_DATA_PATH, "internal")
    if os.path.exists(internal_path):
        shutil.rmtree(internal_path)


def read_report(reports_base_path, report_name, date):
    """
    Reads a single output report CSV (written by Spark as one or more
    part-files within a dated folder) into a pandas dataframe, for
    verification purposes.
    """
    folder = os.path.join(reports_base_path, report_name, f"{report_name}_{date}")
    csv_files = glob.glob(os.path.join(folder, "*.csv"))
    return pd.read_csv(csv_files[0])


def get_campaign_value(df, campaign_name, column):
    """Returns a specific column value for a named campaign."""
    return df[df["campaign_name"] == campaign_name][column].iloc[0]


def run_assertions(reports_path):
    """
    Verifies the integration test's expected outcomes: that completion
    percentage correctly remains unchanged when a campaign step is
    added with no new user activity, and that the campaign overview
    correctly reflects the updated step count.
    """
    checks = []

    day1_engagement = read_report(reports_path, "current_campaign_engagement_report", "20230721")
    day2_engagement = read_report(reports_path, "current_campaign_engagement_report", "20230722")
    day1_overview = read_report(reports_path, "campaign_overview", "20230721")
    day2_overview = read_report(reports_path, "campaign_overview", "20230722")

    checks.append((
        "Day 1 completion is 1.0 (user completed all 3 steps)",
        get_campaign_value(day1_engagement, "win_back", "average_percent_completion") == 1.0
    ))
    checks.append((
        "Day 2 completion remains 1.0 despite 2 steps being added",
        get_campaign_value(day2_engagement, "win_back", "average_percent_completion") == 1.0
    ))
    checks.append((
        "Day 1 campaign overview shows 3 steps",
        get_campaign_value(day1_overview, "win_back", "number_of_steps") == 3
    ))
    checks.append((
        "Day 2 campaign overview shows 5 steps",
        get_campaign_value(day2_overview, "win_back", "number_of_steps") == 5
    ))

    print("\nIntegration test results:")
    all_passed = True
    for description, passed in checks:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {description}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\nAll checks passed.")
    else:
        print("\nOne or more checks failed.")
        sys.exit(1)


def run_integration_test():
    """
    Runs the full daily campaign pipeline end-to-end against small,
    local mock data across two days, to verify the pipeline logic conforms
    to the technical assessment's requirements:
    - Successful execution
    - Correct report outputs
    - Correct handling of campaign steps added mid-campaign (steps
    added on day 2 should not retroactively affect a user's completion
    percentage for activity that occurred on day 1)
    """
    clean_internal_state()

    print("Running pipeline for day 1 (2023-07-21)...")
    main(bucket_name="test-bucket", date="20230721", base_path=TEST_DATA_PATH)

    print("Running pipeline for day 2 (2023-07-22, 2 campaign steps added, no new engagement)...")
    main(bucket_name="test-bucket", date="20230722", base_path=TEST_DATA_PATH)

    print("Integration test pipeline runs complete. Verifying results...")
    run_assertions(os.path.join(TEST_DATA_PATH, "output", "reports"))


if __name__ == "__main__":
    run_integration_test()