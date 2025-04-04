#!/bin/bash
clear

echo "🚀 Starting french Courses Enrollments Project Setup!"

# Add these lines to stop and remove any existing kestra container.
echo "🔹 Stopping and removing any existing Kestra container..."
docker stop kestra 2>/dev/null
docker rm kestra 2>/dev/null

gcloud auth revoke --all

# ✅ Fix permissions on the host machine (Linux/macOS only)
echo "🔹 Fixing permissions for airflow/dags/dbt..."
mkdir -p dbt  # Ensure the folder exists

# Detect OS type
OS_TYPE=$(uname -s)

if [[ "$OS_TYPE" == "Darwin" ]]; then
    # macOS: Use `id -gn` to get the group name
    PRIMARY_GROUP=$(id -gn)
    chown -R $(whoami):$PRIMARY_GROUP dbt
    chmod -R 775 dbt

    mkdir -p dbt/logs
    chown -R $(whoami):$PRIMARY_GROUP dbt/logs
    chmod -R 777 dbt/logs

elif [[ "$OS_TYPE" == "Linux" ]]; then
    # Linux: Use `id -g` to get the group ID
    PRIMARY_GROUP=$(id -g)
    chown -R $(whoami):$PRIMARY_GROUP dbt
    chmod -R 775 dbt

    mkdir -p dbt/logs
    chown -R $(whoami):$PRIMARY_GROUP dbt/logs
    chmod -R 777 dbt/logs

elif [[ "$OS_TYPE" == "MINGW64_NT"* || "$OS_TYPE" == "CYGWIN_NT"* || "$OS_TYPE" == "MSYS_NT"* ]]; then
    # Windows (Git Bash, WSL, Cygwin, or MSYS)
    echo "🛑 Skipping permission changes on Windows (chown not supported)."
else
    echo "⚠ Unknown OS: $OS_TYPE - Skipping permission changes."
fi

echo "✅ Permissions set for dbt and dbt/logs (if applicable)."


# Ensure script runs in its directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit

# Create & Write Environment Variables
echo "🔹 Creating .env file..."
rm -f .env
cat > .env <<EOL
GCP_PROJECT_ID=$GCP_PROJECT_ID
GCP_LOCATION=europe-west1
#KESTRA_API_URL=http://localhost:8080/api/v1
#KESTRA_NAMESPACE=france-courses-enrollments-t
BIGQUERY_SOURCE_DATASET=source_tables
BIGQUERY_EXTERNAL_DATASET=external
#DBT_CLOUD_ACCOUNT=france-market-research
#DBT_CLOUD_PROJECT=france-courses-enrollments # Replace with your dbt project
EOL

echo "✅ .env file created successfully."

# Create secrets folder
mkdir -p secrets
rm -f secrets/gcp_credentials.json

# Authenticate with Google Cloud
echo "🔹 Authenticating with Google Cloud..."
gcloud auth login || { echo "❌ GCP login failed! Exiting..."; exit 1; }

# Get current user email
USER_EMAIL=$(gcloud config get-value account)
echo "🔹 You are logged in as: $USER_EMAIL"

# Select or create GCP project
echo "🔹 Fetching available GCP projects..."
gcloud projects list --format="table(projectId, name)"

while true; do
  read -p "Enter an existing GCP Project ID (or press Enter to create a new one): " GCP_PROJECT_ID

  if [ -z "$GCP_PROJECT_ID" ]; then
    read -p "Enter a new GCP Project ID: " GCP_PROJECT_ID
    echo "🔹 Creating new project: $GCP_PROJECT_ID..."
    
    gcloud projects create $GCP_PROJECT_ID --name="$GCP_PROJECT_ID" --set-as-default
    PROJECT_EXISTS=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectId)" 2>/dev/null)
    
    if [ -z "$PROJECT_EXISTS" ]; then
      echo "❌ Failed to create project. Please check permissions."
      exit 1
    fi
    echo "✅ Project created successfully!"
    break
  else
    PROJECT_EXISTS=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectId)" 2>/dev/null)
    if [ -n "$PROJECT_EXISTS" ]; then
      echo "✅ Using existing project: $GCP_PROJECT_ID"
      break
    else
      echo "❌ Project '$GCP_PROJECT_ID' does not exist."
    fi
  fi
done

# Set the project
gcloud config set project $GCP_PROJECT_ID
echo "GCP_PROJECT_ID=$GCP_PROJECT_ID" >> .env

# Check Billing
echo "🔹 Checking if billing is enabled..."
BILLING_STATUS=$(gcloud billing projects describe $GCP_PROJECT_ID --format="value(billingEnabled)")
if [ "$BILLING_STATUS" != "True" ]; then
   echo "❌ Billing is not enabled for this project!"
  echo "To proceed, you must enable billing manually."

  # Show available billing accounts
  echo "🔹 Available billing accounts:"
  gcloud billing accounts list

  echo "🔹 Follow these steps to enable billing:"
  echo "   1️⃣ Go to the Billing Console: https://console.cloud.google.com/billing"
  echo "   2️⃣ Select or link a billing account to your project: $GCP_PROJECT_ID"
  echo "   3️⃣ Once billing is activated, re-run this script."

  exit 1
fi

echo "✅ Billing is enabled."

# Create Service Account
SERVICE_ACCOUNT_NAME="french-courses-enrollments"
SERVICE_ACCOUNT_EMAIL="$SERVICE_ACCOUNT_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"
KEY_FILE_PATH="secrets/gcp_credentials.json"

EXISTING_SA=$(gcloud iam service-accounts list --format="value(email)" --filter="email:$SERVICE_ACCOUNT_EMAIL")

if [ -z "$EXISTING_SA" ]; then
    echo "🔹 Creating service account..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME --display-name "Service Account for GCS Access"
fi

# Assign IAM roles (always)
echo "🔹 Assigning/Ensuring IAM roles..."
for role in "roles/storage.admin" "roles/bigquery.admin" "roles/iam.serviceAccountAdmin"; do
    gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="$role"
done


# Generate service account key
echo "🔹 Generating service account key..."
rm -f secrets/gcp_credentials.json

# Attempt to create key, handle max key error
if ! gcloud iam service-accounts keys create secrets/gcp_credentials.json --iam-account=$SERVICE_ACCOUNT_EMAIL; then
    # if the error is a precondition failure, try deleting old keys.
    if [[ "$?" -eq 1 ]]; then
        echo "🔹 Max number of keys reached, deleting old keys..."
        KEY_IDS=$(gcloud iam service-accounts keys list --iam-account=$SERVICE_ACCOUNT_EMAIL --format='value(name)')
        for KEY_ID in $KEY_IDS; do
            gcloud iam service-accounts keys delete $KEY_ID --iam-account=$SERVICE_ACCOUNT_EMAIL -q
        done
        # try to create the key again.
        if ! gcloud iam service-accounts keys create secrets/gcp_credentials.json --iam-account=$SERVICE_ACCOUNT_EMAIL; then
            echo "❌ Failed to create service account key after key deletion."
            exit 1
        fi
    else
        echo "❌ Failed to create service account key. Precondition check failed."
        exit 1
    fi
fi

chmod 644 $KEY_FILE_PATH

# Add a delay here to allow the key to propagate
echo "⏳ Waiting for key propagation (30 seconds)..."
sleep 30

# Validate key file
if [ ! -f "$KEY_FILE_PATH" ]; then
    echo "❌ Failed to create service account key."
    exit 1
fi

# Try to activate service account (Detect JWT Error)
echo "🔹 Activating service account..."
if ! gcloud auth activate-service-account --key-file=$KEY_FILE_PATH 2>&1 | tee activation_log.txt | grep -q "invalid_grant"; then
    echo "✅ Service account activated successfully."
else
    echo "❌ Invalid JWT Signature detected! Recreating key..."
    
    # Delete old key and regenerate
    gcloud iam service-accounts keys delete $(jq -r .private_key_id < $KEY_FILE_PATH) --iam-account=$SERVICE_ACCOUNT_EMAIL -q
    rm -f $KEY_FILE_PATH
    
    gcloud iam service-accounts keys create $KEY_FILE_PATH --iam-account=$SERVICE_ACCOUNT_EMAIL
    chmod 644 $KEY_FILE_PATH

    # Retry activation
    if ! gcloud auth activate-service-account --key-file=$KEY_FILE_PATH; then
        echo "❌ Service account activation failed again. Please check permissions manually."
        exit 1
    fi
fi

echo "✅ GCP authentication complete."

# Store service account credentials path in .env
echo "GOOGLE_APPLICATION_CREDENTIALS=$KEY_FILE_PATH" >> .env
echo "SERVICE_ACCOUNT_EMAIL=$SERVICE_ACCOUNT_EMAIL" >> .env

# ✅ Load and Export Variables
export $(grep -v '^#' .env | xargs)

# Ensure required variables are set
if [ -z "$SERVICE_ACCOUNT_EMAIL" ] || [ -z "$GCP_PROJECT_ID" ]; then
    echo "❌ Required variables are missing. Exiting..."
    exit 1
fi

# 7️⃣ Deploy Infrastructure with Terraform
echo "🔹 Deploying infrastructure with Terraform..."
cd terraform
terraform init
terraform apply -auto-approve \
  -var="project_id=$GCP_PROJECT_ID" \
  -var="region=europe-west1" \
  -var="bucket_name=$(whoami)-${GCP_PROJECT_ID}" \
  -var="service_account_email=$SERVICE_ACCOUNT_EMAIL"

if [ $? -ne 0 ]; then
    echo "❌ Terraform deployment failed! Exiting..."
    exit 1
fi

echo "✅ Terraform deployment complete."

# ✅ # Extract Terraform Outputs
GCP_BUCKET_NAME=$(terraform output -raw bucket_name)
GCP_CREDS_JSON=$(terraform output -json GCP_CREDS | jq -r tostring)

# ✅ Ensure `GCS_BUCKET` is added to `.env`
if grep -q "^GCS_BUCKET=" ../.env; then
    sed -i "s|^GCS_BUCKET=.*|GCS_BUCKET=$GCS_BUCKET_NAME|" ../.env
else
    echo "GCS_BUCKET=$GCS_BUCKET_NAME" >> ../.env
fi

echo "✅ GCS_BUCKET added to .env: $GCS_BUCKET_NAME"

cd ..

# Echo the variables before sed commands
echo "GCP_CREDS_JSON: $GCP_CREDS_JSON"
echo "GCP_PROJECT_ID: $GCP_PROJECT_ID"
echo "GCP_LOCATION: $GCP_LOCATION"
echo "GCP_BUCKET_NAME: $GCP_BUCKET_NAME"
echo "GCP_DATASET: $BIGQUERY_SOURCE_DATASET"

# Replace Placeholders in 01_gcp_kv.yaml
echo "🔹 Replacing placeholders in 01_gcp_kv.yaml..."
# Use Perl and sed commands to replace placeholders and save to a new file
#perl -pe "s|gcp_creds_value|$GCP_CREDS_JSON|g" 01_gcp_kv.yaml > new_01_gcp_kv.yaml
sed "s|gcp_project_id_value|$GCP_PROJECT_ID|g" 01_gcp_kv.yaml > new_01_gcp_kv.yaml
sed "s|gcp_location_value|$GCP_LOCATION|g" new_01_gcp_kv.yaml > temp.yaml && mv temp.yaml new_01_gcp_kv.yaml
sed "s|gcp_bucket_name_value|$GCP_BUCKET_NAME|g" new_01_gcp_kv.yaml > temp.yaml && mv temp.yaml new_01_gcp_kv.yaml

# 8️⃣ Start Docker & Kestra
echo "🔹 Starting Docker services... THIS WILL TAKE SOMETIME!"
docker-compose build --no-cache
docker-compose up -d


# 9️⃣ Wait for Kestra
echo "⏳ Waiting for Kestra to initialize..."
sleep 120  

# 🔟 Kestra workflow import.
echo "🔹 Importing Kestra workflows..."
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@new_01_gcp_kv.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@02_courses_enrollments_pipeline.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@03_formacode_pipeline.yaml

# Display URLs
echo "✅ Setup Complete!"
echo "📊 Visit Kestra: http://localhost:8080"

# Prompt for Terraform Destroy
read -p "Press Enter to destroy the GCS bucket and BigQuery datasets (or Ctrl+C to skip)..."

# Delete the new_01_gcp_kv.yaml file
echo "🔹 Deleting the new_01_gcp_kv.yaml file..."
rm -f new_01_gcp_kv.yaml
echo "✅ new_01_gcp_kv.yaml deleted."

# Destroy Terraform Resources
echo "🔹 Destroying Terraform resources..."
cd terraform
terraform destroy -auto-approve \
    -var="project_id=$GCP_PROJECT_ID" \
    -var="region=europe-west1" \
    -var="bucket_name=$GCP_BUCKET_NAME" \
    -var="service_account_email=$SERVICE_ACCOUNT_EMAIL"

if [ $? -ne 0 ]; then
    echo "❌ Terraform destroy failed!"
else
    echo "✅ Terraform resources destroyed successfully."
fi

cd ..