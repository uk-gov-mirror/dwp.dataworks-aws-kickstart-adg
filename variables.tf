variable "assume_role" {
  type        = string
  default     = "ci"
  description = "IAM role assumed by Concourse when running Terraform"
}

variable "region" {
  type    = string
  default = "eu-west-2"
}

variable "truststore_aliases" {
  description = "comma seperated truststore aliases"
  type        = list(string)
  default     = ["dataworks_root_ca", "dataworks_mgt_root_ca"]
}

variable "emr_release" {
  default = {
    development = "6.2.0"
    qa          = "6.2.0"
    integration = "6.2.0"
    preprod     = "6.2.0"
    production  = "6.2.0"
  }
}

variable "emr_instance_type" {
  default = {
    development = "m5.2xlarge"
    qa          = "m5.2xlarge"
    integration = "m5.2xlarge"
    preprod     = "m5.2xlarge"
    production  = "m5.2xlarge"
  }
}

variable "emr_core_instance_count" {
  default = {
    development = "1"
    qa          = "1"
    integration = "1"
    preprod     = "1"
    production  = "2"
  }
}

variable "num_cores_per_executor"{
  default = {
    development = "5"
    qa          = "5"
    integration = "5"
    preprod     = "5"
    production  = "5"
  }
}

variable "num_executors_per_node"{
  default = {
    development = "1"
    qa          = "1"
    integration = "1"
    preprod     = "1"
    production  = "1"
  }
}

variable "ram_memory_per_node"{
  default = {
    development = "32"
    qa          = "32"
    integration = "32"
    preprod     = "32"
    production  = "32"
  }
}

variable "emr_num_cores_per_core_instance" {
  default = {
    development = "8"
    qa          = "8"
    integration = "8"
    preprod     = "8"
    production  = "8"
  }
}

variable "spark_kyro_buffer" {
  default = {
    development = "128"
    qa          = "128"
    integration = "128"
    preprod     = "2047m"
    production  = "2047m"
  }
}


variable "source_bucket_prefix" {
  default = {
    development = "dev"
    qa          = "qa"
    integration = "qa2"
    preprod     = "stage"
    production  = "prod"
  }
}

# Note this isn't the amount of RAM the instance has; it's the maximum amount
# that EMR automatically configures for YARN. See
# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html
# (search for yarn.nodemanager.resource.memory-mb)
variable "emr_yarn_memory_gb_per_core_instance" {
  default = {
    development = "120"
    qa          = "120"
    integration = "120"
    preprod     = "184"
    production  = "184"
  }
}

variable "emr_ami_id" {
  description = "AMI ID to use for the HBase EMR nodes"
  default     = "ami-0672faa58b65ff88d"
}

variable "metadata_store_adg_writer_username" {
  description = "Username for metadata store write RDS user"
  default     = "kickstart-adg-writer"
}

variable "source_assume_role_name" {
  description = "Assume role name from source"
  default     = "dataworks-ksr"
}
