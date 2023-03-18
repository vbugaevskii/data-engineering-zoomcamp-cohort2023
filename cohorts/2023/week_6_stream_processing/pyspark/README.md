## PySpark

You can find basic instructions of how to configure PySpark cluster at Yandex.Cloud DataProc [here](https://github.com/vbugaevskii/data-engineering-zoomcamp-cohort2023/blob/main/cohorts/2023/week_5_batch_processing/README.md). 

This is document has some extra notes about configuring PySpark cluster for interaction with Kafka cluster. The main problem you can face with is the `kafka-clients.jar` libarary incompatability. There are several symptom of this problem:
- `org.apache.kafka.common.config.ConfigException: Missing required configuration "partition.assignment.strategy" which has no default value`;
- `java.lang.NoSuchMethodError: 'void org.apache.kafka.clients.consumer.KafkaConsumer.subscribe(java.util.Collection)'`.

The 1st error can be partialy solved setting the option `kafka.partition.assignment.strategy` to sove values, e.g., `range` or `roundrobin`. However, the 2nd error will appear.

The 2nd error is the most distinct sign you have the wrong jar-libarary. You can try to solve this problem passing the right version of library using `spark.jars.packages` or `spark.jars` options, but in my case it didn't help.

### Solution

I had to forcely update jars at namenode and datanodes machines of DataProc:

```
# connect to vm
ssh ubuntu@c1a-dataproc-m-4mwfijpjiwgk8hbg.mdb.yandexcloud.net

# get root permissions
sudo su

# download relevenat libraries
cd /usr/lib/spark/jars

wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.2/spark-sql-kafka-0-10_2.12-3.0.2.jar
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.2/spark-token-provider-kafka-0-10_2.12-3.0.2.jar

# restart service
systemctl restart hadoop-yarn@nodemanager.service       # if it's datanode
systemctl restart hadoop-yarn@resourcemanager.service   # if it's namenode
```

When you have done these steps you possibly can get `org.apache.kafka.common.KafkaException: Failed to construct kafka consumer` error. Than you have to read the stacktrace and change your configuration the right way.
