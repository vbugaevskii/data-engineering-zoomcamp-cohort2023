## Yandex.Cloud 

Unfortunatelly Google Cloud and AWS are not available in my country, so I will use Yandex.Cloud.

**Note:** all credentials below are not valid.

### Pre-Requisites

1. Terraform client installation: https://www.terraform.io/downloads
2. Yandex.Cloud CLI: https://cloud.yandex.ru/docs/cli/quickstart

### Preprare Cloud

1. Go to [Yandex.Cloud console](https://console.cloud.yandex.ru/) and create a new cloud for course, e.g. `data-engineering-zoomcamp`.

2. Create a service account, e.g. `storage-admin-sa` with:
- `storage.configurer` and `storage.admin` roles for buckets, see [here](https://cloud.yandex.ru/docs/iam/concepts/access-control/roles) and [here](https://cloud.yandex.ru/docs/storage/security/);
- `mdb.admin` role for ClickHouse (closest analogue to BigQuery), see [here](https://cloud.yandex.ru/docs/iam/concepts/access-control/roles) and [here](https://cloud.yandex.ru/docs/managed-clickhouse/security/).

3. Prepare credentials from service account for CLI: use [tutorial](https://cloud.yandex.ru/docs/tutorials/infrastructure-management/terraform-quickstart#get-credentials):
    
    **Note:** you can use the commands to view your resources:
    
    ```
    $ yc resource-manager cloud list
    
    +----------------------+---------------------------+----------------------+
    |          ID          |           NAME            |   ORGANIZATION ID    |
    +----------------------+---------------------------+----------------------+
    | b1gfog90cahpmkosk5cp | data-engineering-zoomcamp | bpfc7863kge62q9246j3 |
    +----------------------+---------------------------+----------------------+
    ```
    <br>
    
    ```
    $ yc resource-manager --cloud-id b1gfog90cahpmkosk5cp folder list
    
    +----------------------+---------+--------+--------+
    |          ID          |  NAME   | LABELS | STATUS |
    +----------------------+---------+--------+--------+
    | b1gjidbal573f10rma62 | default |        | ACTIVE |
    +----------------------+---------+--------+--------+
    ```
    <br>

    1. Create authorization key, see [the link](https://cloud.yandex.ru/docs/iam/concepts/authorization/key):
    <br></br>
    
    ```bash
    $ yc iam key create \
        --service-account-id ajep0cad7suh90d8q7uh \
        --folder-id b1gjidbal573f10rma62 \
        --output storage-admin-sa-key.json
    
    id: ajejoioss2g1pq7qkn1b
    service_account_id: ajep0cad7suh90d8q7uh
    created_at: "2023-01-28T22:12:07.893670584Z"
    key_algorithm: RSA_2048
    ```
    <br>
    
    2. Create static access key, see [the link](https://cloud.yandex.ru/docs/iam/operations/sa/create-access-key):
    <br></br>
    
    ```bash
    $ yc iam access-key create \
        --service-account-id ajep0cad7suh90d8q7uh \
        --folder-id b1gjidbal573f10rma62
    
    access_key:
      id: ajenijmpqfsfulg01756
      service_account_id: ajep0cad7suh90d8q7uh
      created_at: "2023-01-28T23:50:41.624754832Z"
      key_id: YCAJEEBSJpx32jkPwmktbbVkr
    secret: YCO4G6MZkme8OwW9Yls22e_T89BWLga8j9Fexm4D
    ```
    
### Terraform commands

* `terraform init` – install provider plugins;
* `terraform plan` – preview execution pipeline;
* `terraform apply` – run execution pipeline;
* `terraform destroy` – destroy all objects created by pipeline.
