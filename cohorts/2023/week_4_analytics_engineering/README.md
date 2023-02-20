## dbt

Unfortunatelly Google Cloud and AWS are not available in my country, so I will use Yandex.Cloud. I will use ClickHouse instead of BigQuery and DataLens for visualisation.

### Useful Tips

1. Create environment to run dbt:
```bash
conda create -n de-zoomcamp-dbt python=3.8
conda activate de-zoomcamp-dbt
pip install -r requirements.txt
```

2. dbt commands
```bash
dbt --version                                              # check dbt installed
dbt init                                                   # create project
dbt debug                                                  # check connection is OK
dbt compile                                                # compile sql queries
dbt run                                                    # run all models
dbt run -m stg_fhv_tripdata                                # run model "stg_fhv_tripdata"
dbt run -m stg_green_tripdata --var 'is_test_run: false'   # run model NOT in test mode
dbt deps                                                   # install deps
dbt seed --full-refresh                                    # add csv files to dbt
```

3. Useful Docs:
- https://docs.getdbt.com/docs/get-started/connection-profiles
- https://docs.getdbt.com/reference/warehouse-setups/clickhouse-setup
- https://docs.getdbt.com/reference/resource-configs/clickhouse-configs

4. Useful Tutorials:
- https://www.youtube.com/watch?v=btaH7P0U_2g
- https://www.youtube.com/watch?v=16VBGdNuHv8

5. You can find the report by name `report.png`. It's not very nice actually.
