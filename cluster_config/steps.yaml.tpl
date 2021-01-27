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

Steps:
- Name: "submit-job"
  HadoopJarStep:
    Args:
    - "spark-submit"
    - "--master"
    - "yarn"
    - "--conf"
    - "spark.yarn.submit.waitAppCompletion=true"
    - "--py-files"
    - "s3://${s3_config_bucket}/component/kickstart-analytical-dataset-generation/steps/spark/jobs.zip"
    - "s3://${s3_config_bucket}/component/kickstart-analytical-dataset-generation/steps/spark/main.py"
    Jar: "command-runner.jar"
  ActionOnFailure: "${action_on_failure}"
