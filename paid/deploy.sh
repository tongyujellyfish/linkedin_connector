#!/usr/bin/env bash

gcloud config set project linkedin-327306
gcloud services enable cloudbuild.googleapis.com
gcloud functions deploy linked_paid \
     --entry-point main \
    --runtime python38 \
    --trigger-http \
    --service-account linkedin-connector@linkedin-327306.iam.gserviceaccount.com \
    --verbosity debug \
    --timeout 540s \
    --memory 512MB   \
    --set-env-vars USE_WORKER_V2=TRUE \
