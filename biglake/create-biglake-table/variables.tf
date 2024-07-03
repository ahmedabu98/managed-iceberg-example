# Variables to be used for the BigLake Metastore script in ./main.tf

variable "project" {
  description = "The project in which the BigLake Metastore resources will be created."
  type        = string
  default     = "apache-beam-testing"
}

variable "location" {
  description = "The GCP region in which the BigLake Metastore resources will be created."
  type        = string
  default     = "us-central1"
}

variable "catalog" {
  description = "The name of the BigLake Metastore catalog."
  type        = string
  default     = "ahmed_catalog"
}

variable "database" {
  description = "The name of the BigLake Metastore database."
  type        = string
  default     = "my_database"
}

variable "table" {
  description = "The name of the BigLake Metastore table."
  type        = string
  default     = "my_table"
}
