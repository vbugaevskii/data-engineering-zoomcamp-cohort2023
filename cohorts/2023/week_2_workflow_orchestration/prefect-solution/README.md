## Prefect

Unfortunatelly Google Cloud and AWS are not available in my country, so I will use Yandex.Cloud.

### Useful Tips

1. Create environment to run Prefect:
```bash
conda create -n de-zoomcamp python=3.8
conda activate de-zoomcamp
pip install -r requirements.txt
```

2. Prefect Commands
```bash
# create deploymnet with cron schedule
prefect deployment build flows/etl_web_to_gcs.py:etl_web_to_gcs -n etl_web_to_gcs --cron '0 5 1 * *'
prefect deployment apply etl_web_to_gcs-deployment.yaml

# create deployment with params as json
prefect deployment build flows/etl_gcs_to_bq.py:etl_gcs_to_bq_multiple -n etl_gcs_to_bq_multiple --params '{"months": [2, 3], "year": 2019, "color": "yellow"}'
prefect deployment apply etl_gcs_to_bq_multiple-deployment.yaml

# create deployment with modified params
prefect deployment build flows/etl_web_to_gcs_github.py:etl_gcs_to_bq_github -n etl_gcs_to_bq_github --param month=4 --param year=2019 --param color='green'
prefect deployment apply etl_gcs_to_bq_github-deployment.yaml

# start web viewer
prefect orion start

# start prefect agent for running deployments
prefect agent start --work-queue "default"
```

3. `yandex-cloud-s3-credentials` block configuration:
  - Yandex.Cloud is compatible with AWS interface;
  - you will need [service account](https://cloud.yandex.ru/docs/iam/concepts/users/service-accounts) from previous homework and [access key](https://cloud.yandex.ru/docs/iam/concepts/authorization/access-key) for it;
  - use endpoint: https://storage.yandexcloud.net

4. `yandex-cloud-s3-bucket` block configuration:
  - use AWS credentials from previous step;
  - use endpoint: https://storage.yandexcloud.net

5. `yandex-cloud-clickhouse-connector` block configuration:
  - ClickHouse is used instead of BigQuery;
  - you will need [the certificate](https://cloud.yandex.ru/docs/managed-clickhouse/quickstart#connect) to connect ClickHouse;
  - if you work with clickhouse in remote mode, you have to make it's host public, see [link](https://cloud.yandex.ru/docs/managed-clickhouse/operations/hosts#update);
  - try to [connect localy](https://cloud.yandex.ru/docs/managed-clickhouse/operations/connect) using `clickhouse-driver` package for python;
  - try to [connect localy](https://clickhouse-sqlalchemy.readthedocs.io/en/latest/connection.html#http) using `clickhouse-sqlalchemy` package for python;
  - use URI:
    ```
    clickhouse+http://admin:password@c-<clickhouse-cluster-id>.rw.mdb.yandexcloud.net:8443/default?protocol=https&verify=YandexCA.crt
    ```
    and additional connection arguments:
    ```json
    {
        "protocol": "https",
        "verify": "YandexCA.crt"
    }
    ```
    where `YandexCA.crt` is the certificate from previous bullets.
    
6. `slack-alerts` block configuration:
  - create your own [app](https://api.slack.com/apps) in Slack;
  - create incoming [web hooks](https://api.slack.com/messaging/webhooks) for your app.

7. slack notifications:
  - use web hooks (not incoming web hooks);
  - pay attention to [the tutorial](https://medium.com/the-prefect-blog/sending-slack-notifications-in-python-with-prefect-840a895f81c).
