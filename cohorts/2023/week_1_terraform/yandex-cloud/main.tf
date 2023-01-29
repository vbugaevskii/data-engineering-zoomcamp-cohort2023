terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"
}

# Ref: https://registry.terraform.io/providers/yandex-cloud/yandex/latest/docs
# Ref: https://cloud.yandex.ru/docs/iam/operations/authorized-key/create
provider "yandex" {
  service_account_key_file = "storage-admin-sa-key.json"
  cloud_id  = "b1gfog90cahpmkosk5cp"
  folder_id = "b1gjidbal573f10rma62"
  zone      = "ru-central1-a"
}

# Create bucket
# Ref: https://registry.terraform.io/providers/yandex-cloud/yandex/latest/docs/resources/storage_bucket
# Ref: https://cloud.yandex.ru/docs/iam/operations/sa/create-access-key
# Ref: https://cloud.yandex.ru/docs/storage/operations/buckets/create
# TODO: Think how credentials can be stored in a safe way
resource "yandex_storage_bucket" "bucket-dev" {
  access_key = "<access_key>"
  secret_key = "<secret_key>"
  bucket     = "tf-bucket-dev"
}

# Create Clickhouse instead of BigQuery. BigQuery is only available at GCP
# Ref: https://registry.terraform.io/providers/yandex-cloud/yandex/latest/docs/resources/mdb_clickhouse_cluster
resource "yandex_mdb_clickhouse_cluster" "ch_database" {
  name        = "ch-db-dev"
  environment = "PRESTABLE"
  network_id  = "enp5e3e9c5cu37nuh19r"

  clickhouse {
    resources {
      resource_preset_id = "b1.micro"
      disk_type_id       = "network-ssd"
      disk_size          = 10
    }
  }

  database {
    name = "db_dataset"
  }

  # TODO: Think how credentials can be stored in a safe way
  user {
    name     = "ch_user"
    password = "ch_strong_password"
    permission {
      database_name = "db_dataset"
    }
  }

  host {
    type = "CLICKHOUSE"
    zone = "ru-central1-a"
  }

  cloud_storage {
    enabled = false
  }

  maintenance_window {
    type = "ANYTIME"
  }
}

