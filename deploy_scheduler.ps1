# deploy_scheduler.ps1
# ====================
# Deploys Cloud Functions + sets up Cloud Scheduler for full pipeline automation.

$PROJECT    = "citibike-pipeline-499418"
$REGION     = "us-east1"
$BUCKET     = "citibike-pipeline-499418-data"
$GCLOUD     = "C:\Users\User\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
$BASE       = "V:\Data management 2\citibike-pipeline"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Citi Bike Pipeline - Scheduler Setup  " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# ── Enable APIs ──────────────────────────────────────────────
Write-Host "[1/6] Enabling required GCP APIs..." -ForegroundColor Yellow
& $GCLOUD services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com cloudscheduler.googleapis.com run.googleapis.com --project=$PROJECT
Write-Host "      APIs enabled." -ForegroundColor Green

# ── Deploy GBFS Cloud Function ────────────────────────────────
Write-Host ""
Write-Host "[2/6] Deploying Cloud Function: gbfs-ingest..." -ForegroundColor Yellow
& $GCLOUD functions deploy gbfs-ingest `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source="$BASE\cloud_functions\gbfs_ingest" `
    --entry-point=gbfs_ingest `
    --trigger-http `
    --allow-unauthenticated `
    --memory=256MB `
    --timeout=60s `
    --project=$PROJECT

if ($LASTEXITCODE -eq 0) {
    Write-Host "      gbfs-ingest deployed OK." -ForegroundColor Green
} else {
    Write-Host "      gbfs-ingest deploy FAILED. Check output above." -ForegroundColor Red
    exit 1
}

# Get the function URL
$GBFS_URL = (& $GCLOUD functions describe gbfs-ingest --region=$REGION --project=$PROJECT --format="value(serviceConfig.uri)" 2>$null)
Write-Host "      URL: $GBFS_URL" -ForegroundColor Gray

# ── Upload dbt project zip ────────────────────────────────────
Write-Host ""
Write-Host "[3/6] Packaging dbt project and uploading to GCS..." -ForegroundColor Yellow
$zipPath = "$env:TEMP\dbt_project.zip"
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path "$BASE\dbt\*" -DestinationPath $zipPath
& $GCLOUD storage cp $zipPath "gs://$BUCKET/dbt_project/dbt_project.zip" --project=$PROJECT
Write-Host "      dbt project uploaded." -ForegroundColor Green

# ── Deploy dbt Cloud Function ─────────────────────────────────
Write-Host ""
Write-Host "[4/6] Deploying Cloud Function: dbt-run..." -ForegroundColor Yellow
& $GCLOUD functions deploy dbt-run `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source="$BASE\cloud_functions\dbt_run" `
    --entry-point=dbt_run `
    --trigger-http `
    --allow-unauthenticated `
    --memory=512MB `
    --timeout=3600s `
    --project=$PROJECT

if ($LASTEXITCODE -eq 0) {
    Write-Host "      dbt-run deployed OK." -ForegroundColor Green
} else {
    Write-Host "      dbt-run deploy FAILED. Check output above." -ForegroundColor Red
}

$DBT_URL = (& $GCLOUD functions describe dbt-run --region=$REGION --project=$PROJECT --format="value(serviceConfig.uri)" 2>$null)
Write-Host "      URL: $DBT_URL" -ForegroundColor Gray

# ── Cloud Scheduler: GBFS every 15 min ───────────────────────
Write-Host ""
Write-Host "[5/6] Creating Cloud Scheduler jobs..." -ForegroundColor Yellow

& $GCLOUD scheduler jobs delete gbfs-every-15min --location=$REGION --project=$PROJECT --quiet 2>$null
& $GCLOUD scheduler jobs create http gbfs-every-15min `
    --location=$REGION `
    --schedule="*/15 * * * *" `
    --uri=$GBFS_URL `
    --http-method=GET `
    --time-zone="America/New_York" `
    --description="Fetch Citi Bike GBFS real-time station status every 15 minutes" `
    --project=$PROJECT
Write-Host "      [OK] gbfs-every-15min created" -ForegroundColor Green

# ── Cloud Scheduler: Spark daily at 2am ──────────────────────
$DATAPROC_URL = "https://dataproc.googleapis.com/v1/projects/$PROJECT/locations/$REGION/batches"
$SPARK_BODY   = "{`"pysparkBatch`":{`"mainPythonFileUri`":`"gs://$BUCKET/spark/process_trips.py`"},`"labels`":{`"job`":`"citibike-trips`"},`"runtimeConfig`":{`"version`":`"2.1`"}}"

& $GCLOUD scheduler jobs delete spark-daily --location=$REGION --project=$PROJECT --quiet 2>$null
& $GCLOUD scheduler jobs create http spark-daily `
    --location=$REGION `
    --schedule="0 2 * * *" `
    --uri=$DATAPROC_URL `
    --message-body=$SPARK_BODY `
    --oauth-service-account-email="$PROJECT@appspot.gserviceaccount.com" `
    --time-zone="America/New_York" `
    --description="Run Dataproc Serverless Spark job daily at 2am" `
    --project=$PROJECT
Write-Host "      [OK] spark-daily created" -ForegroundColor Green

# ── Cloud Scheduler: dbt daily at 4am ────────────────────────
& $GCLOUD scheduler jobs delete dbt-daily --location=$REGION --project=$PROJECT --quiet 2>$null
& $GCLOUD scheduler jobs create http dbt-daily `
    --location=$REGION `
    --schedule="0 4 * * *" `
    --uri=$DBT_URL `
    --http-method=GET `
    --time-zone="America/New_York" `
    --description="Run dbt transformations daily at 4am" `
    --project=$PROJECT
Write-Host "      [OK] dbt-daily created" -ForegroundColor Green

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DONE - 3 Scheduler jobs created:" -ForegroundColor Cyan
Write-Host "  gbfs-every-15min  : every 15 min (live data)" -ForegroundColor White
Write-Host "  spark-daily       : 2:00am daily  (process trips)" -ForegroundColor White
Write-Host "  dbt-daily         : 4:00am daily  (refresh marts)" -ForegroundColor White
Write-Host "" -ForegroundColor White
Write-Host "  GCP Console: console.cloud.google.com/cloudscheduler" -ForegroundColor Gray
Write-Host "========================================" -ForegroundColor Cyan
