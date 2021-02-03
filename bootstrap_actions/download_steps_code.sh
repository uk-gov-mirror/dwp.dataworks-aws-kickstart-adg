(
# Import the logging functions
source /opt/emr/logging.sh

function log_wrapper_message() {
  log_adg_message "$${1}" "download_steps_code.sh" "$${PID}" "$${@:2}" "Running as: ,$USER"
}

URL="s3://${s3_bucket_id}/${s3_bucket_prefix}"
Download_DIR=/opt/emr/spark_codes
ZIP_DIR=/opt/emr/spark

echo "Download latest spark codes"
log_wrapper_message "Downloading latest spark codes"

$(which aws) s3 cp "$URL/steps/spark/" $Download_DIR --recursive

echo "SCRIPT_DOWNLOAD_URL: $URL/steps/spark/"

log_wrapper_message "script_download_url: $URL/steps/spark/"

echo "zip the python files location: $ZIP_DIR"

log_wrapper_message "Creating the spark code directory: $ZIP_DIR"

mkdir $ZIP_DIR

log_wrapper_message "Copy main.py file to spark code directory: $ZIP_DIR"

cp "$Download_DIR/main.py" "$ZIP_DIR"

log_wrapper_message "zip dependencies required for main file and copy it in spark code directory: $ZIP_DIR"

cd "$Download_DIR" && zip -x main.py -r "$ZIP_DIR/jobs.zip" .

log_wrapper_message "finished creating executable for spark job ......................."

)  >> /var/log/kickstart_adg/download_steps_code.log 2>&1
