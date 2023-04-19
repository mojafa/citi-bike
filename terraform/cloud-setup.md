PROJECT_ID="de-finnhub"
gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/editor"
gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/viewer"
gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/storage.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/storage.objectAdmin"
gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/bigquery.admin"



PROJECT_ID="de-finnhub"
mkdir ~/.config/gcloud
gcloud iam service-accounts keys create ~/.config/gcloud/terraform.json \
--iam-account=terraform-iam@$PROJECT_ID.iam.gserviceaccount.com
