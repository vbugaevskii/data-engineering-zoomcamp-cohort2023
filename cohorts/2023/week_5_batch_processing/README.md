## PySpark

Unfortunatelly Google Cloud and AWS are not available in my country, so I will use Yandex.Cloud. [Data Proc](https://cloud.yandex.ru/services/data-proc) can be used for managing Spark cluster.

1. You need to [prepare network](https://cloud.yandex.ru/docs/data-proc/tutorials/configure-network), so the cluster can be successfuly created and have [NAT-gateway](https://cloud.yandex.ru/docs/vpc/concepts/gateways#nat-gateway) for Internet access.
2. Create [service account](https://cloud.yandex.ru/docs/data-proc/operations/cluster-create) for Data Proc cluster with roles: [`dataproc.agent`](https://cloud.yandex.ru/docs/data-proc/security/), [`storage.admin`](https://cloud.yandex.ru/docs/data-proc/tutorials/copy-files-from-object-storage).
3. Create ssh key for Data Proc and other VM in the cloud, use `ssh-keygen`.
4. Don't forget to mark `UI Proxy` option for having access to logs.

Now it's possible to [run PySpark jobs](https://cloud.yandex.ru/docs/data-proc/tutorials/pyspark-job-basics), but without having an interactive access using JupyterLab. If you want to run PySpark jobs interactively, you have to follow the [instructions](https://cloud.yandex.ru/docs/data-proc/tutorials/remote-run-job):
1. Create a [virtual machine](https://cloud.yandex.ru/services/compute) using the network and ssh key from previous steps. You can use latest version of Ubuntu. Connect with ssh:
   ```bash
   ssh -i ~/.ssh/dezoomcamp -A vbugaevskii@84.252.143.86
   ```

2. Install Spark with the configuration used in Data Proc:
   * update Ubuntu repositories:
   
     ```
     ssh ubuntu@rc1a-dataproc-m-wukw392m8plsgw7a.mdb.yandexcloud.net \
     "cat /etc/apt/sources.list.d/yandex-dataproc.list" | \
     sudo tee /etc/apt/sources.list.d/yandex-dataproc.list && \
     deb [arch=amd64] http://storage.yandexcloud.net/dataproc/releases/0.2.10 xenial main
     
     ssh ubuntu@rc1a-dataproc-m-wukw392m8plsgw7a.mdb.yandexcloud.net \
     "cat /srv/dataproc.gpg" | sudo apt-key add -
     
     sudo apt update
     ```
   * install Spark:
   
     ```
     sudo apt install openjdk-8-jre-headless hadoop-client hadoop-hdfs spark-core
     ```
   * install Spark python dependencies:
     ```
     wget https://archive.apache.org/dist/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz
     tar -xvzf spark-3.0.3-bin-hadoop3.2.tgz
     sudo cp spark-3.0.3-bin-hadoop3.2/python/ /usr/lib/spark/ -R
     sudo chown root:root /usr/lib/spark/python/ -R
     ```
   * copy the right configuration:
   
     ```
     sudo -E scp -r \
         ubuntu@rc1a-dataproc-m-wukw392m8plsgw7a.mdb.yandexcloud.net:/etc/hadoop/conf/* \
         /etc/hadoop/conf/

     sudo -E scp -r \
         ubuntu@rc1a-dataproc-m-wukw392m8plsgw7a.mdb.yandexcloud.net:/etc/spark/conf/* \
         /etc/spark/conf/
     ```
   * copy Yandex.Cloud java library for working with S3:
   
     ```
     sudo -E scp -r \
         ubuntu@rc1a-dataproc-m-wukw392m8plsgw7a.mdb.yandexcloud.net:/usr/lib/iam-s3-credentials/* \
         /usr/lib/iam-s3-credentials/
     ```
   * copy other external java libraries:
     ```
     sudo -E scp -r \
         ubuntu@rc1a-dataproc-m-wukw392m8plsgw7a.mdb.yandexcloud.net:/usr/lib/spark/external \
         /usr/lib/spark/external
     ```
   * check everything works:
     ```
     spark-submit \
         --master yarn \
         --deploy-mode cluster \
         --class org.apache.spark.examples.SparkPi \
         /usr/lib/spark/examples/jars/spark-examples.jar 1000
     ```
3. Prepare python environment (using conda) where JupyterLab will be run and used for submitting PySpark jobs. Create [requirements.txt](./requirements.txt) file according to [the version of Data Proc](https://cloud.yandex.ru/docs/data-proc/concepts/environment):

   ```
   conda create -n pyspark-jupyter python=3.8 -y
   conda activate pyspark-jupyter
   pip install -r requirements.txt
   ```
4. Run JupyterLab and forward the port from vm:

   ```
   ssh -i ~/.ssh/dezoomcamp -L 8888:localhost:8888 -Nf vbugaevskii@84.252.143.86
   ```
5. Run PySpark jobs using the configuration from [solution-ya.ipynb](./solution-ya.ipynb).
