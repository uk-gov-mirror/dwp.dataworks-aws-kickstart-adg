---
BootstrapActions:
- Name: "start_ssm"
  ScriptBootstrapAction:
    Path: "s3://${s3_config_bucket}/component/kickstart-analytical-dataset-generation/start_ssm.sh"
- Name: "metadata"
  ScriptBootstrapAction:
    Path: "s3://${s3_config_bucket}/component/kickstart-analytical-dataset-generation/metadata.sh"
- Name: "get-dks-cert"
  ScriptBootstrapAction:
    Path: "s3://${s3_config_bucket}/component/kickstart-analytical-dataset-generation/emr-setup.sh"
- Name: "installer"
  ScriptBootstrapAction:
    Path: "s3://${s3_config_bucket}/component/kickstart-analytical-dataset-generation/installer.sh"
- Name: "download_steps_code"
  ScriptBootstrapAction:
    Path: "s3://${s3_config_bucket}/component/kickstart-analytical-dataset-generation/download_steps_code.sh"

Steps:
- Name: "submit-job-vacancy"
  HadoopJarStep:
    Args:
    - "spark-submit"
    - "--master"
    - "yarn"
    - "--conf"
    - "spark.yarn.submit.waitAppCompletion=true"
    - "--py-files"
    - "/opt/emr/spark/jobs.zip"
    - "/opt/emr/spark/main.py"
    Jar: "command-runner.jar"
  ActionOnFailure: "${action_on_failure}"
