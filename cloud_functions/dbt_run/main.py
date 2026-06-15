"""
Cloud Function: dbt_run
========================
Triggered by Cloud Scheduler once daily (after Spark completes).
Downloads the dbt project from GCS, installs dbt-bigquery,
then runs dbt to refresh all transformations and the mart table.

Why run dbt in Cloud Functions?
  - No Docker / Cloud Run setup needed
  - 60-minute timeout is enough for this project's 6 models
  - Reads dbt project from GCS so code changes deploy automatically
"""

import os
import subprocess
import tempfile
import zipfile
import functions_framework
from google.cloud import storage

PROJECT_ID  = "citibike-pipeline-499418"
BUCKET_NAME = "citibike-pipeline-499418-data"
DBT_ZIP_KEY = "dbt_project/dbt_project.zip"    # uploaded by deploy script

DBT_PROFILES = """
citibike_pipeline:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: oauth
      project: citibike-pipeline-499418
      dataset: citibike_staging
      location: US
      threads: 4
      timeout_seconds: 300
"""


@functions_framework.http
def dbt_run(request):
    """Download dbt project from GCS, run dbt, return logs."""
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download and unzip dbt project
            gcs     = storage.Client(project=PROJECT_ID)
            zip_path = os.path.join(tmpdir, "dbt_project.zip")
            gcs.bucket(BUCKET_NAME).blob(DBT_ZIP_KEY).download_to_filename(zip_path)

            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmpdir)

            project_dir = os.path.join(tmpdir, "dbt")

            # Write profiles.yml
            profiles_dir = os.path.join(tmpdir, "profiles")
            os.makedirs(profiles_dir, exist_ok=True)
            with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
                f.write(DBT_PROFILES)

            # Install dbt-bigquery
            subprocess.run(
                ["pip", "install", "dbt-bigquery==1.8.*", "-q"],
                check=True, capture_output=True
            )

            # Run dbt
            result = subprocess.run(
                ["dbt", "run", "--profiles-dir", profiles_dir, "--project-dir", project_dir],
                capture_output=True, text=True, timeout=3000
            )

            log = result.stdout + result.stderr
            print(log)
            status = 200 if result.returncode == 0 else 500
            return (log[-3000:], status)   # return last 3000 chars of logs

    except Exception as e:
        print(f"ERROR: {e}")
        return (f"Error: {e}", 500)
