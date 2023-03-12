## Kafka

I will use Yandex.Cloud with ["Managed Service for Kafka"](https://cloud.yandex.ru/services/managed-kafka), because AWS and GoogleCloud are not available in my country.

1. [Create](https://cloud.yandex.ru/docs/managed-kafka/operations/cluster-create) the [cluster](https://cloud.yandex.ru/docs/managed-kafka/quickstart) with Kafka. Please, pay attention to:
    - **availability zones** – every zone will have a ZooKeeper;   
    - **network** – it should be the same for Spark cluster, if you want them to communicate;
    - **public access** – Kafka cluster will be accessed only using SSL and Yandex ceritificate, if the option is on.

2. You should create [topics](https://cloud.yandex.ru/docs/managed-kafka/operations/cluster-topics) and [users](https://cloud.yandex.ru/docs/managed-kafka/operations/cluster-accounts#grant-permission) for cluster.

3. [Create](https://cloud.yandex.ru/docs/compute/operations/vm-create/create-linux-vm) virtual machine and download the Yandex.Cloud certificate:
   
   ```bash
   wget https://storage.yandexcloud.net/cloud-certs/CA.pem -O YandexCA.crt
   chmod 655 YandexCA.crt
   ```

3. Test connection to Kafka using [kafkacat](https://cloud.yandex.ru/docs/managed-kafka/operations/connect#bash):

   ```bash
   # Install kafkacat
   sudo apt update && sudo apt install -y kafkacat

   # Check the connection
   kafkacat -L -b rc1a-a9585n0rtd07a043.mdb.yandexcloud.net:9091 \
            -X security.protocol=SASL_SSL \
            -X sasl.mechanisms=SCRAM-SHA-512 \
            -X sasl.username=vbugaevskii \
            -X sasl.password=kafkapass \
            -X ssl.ca.location=YandexCA.crt
   ```
   It's not comfortable to pass all security arguments to function. So you can create `~/.config/kafkacat.conf` config file:
   
   ```
   security.protocol=SASL_SSL
   sasl.mechanisms=SCRAM-SHA-512
   sasl.username=KAFKA_USER
   sasl.password=KAFKA_PASS
   ssl.ca.location=/home/vbugaevskii/kafka/YandexCA.crt
   ```
   There are some useful commands with kafkacat:

   ```bash
   # show all topics
   kcat -L -b rc1a-a9585n0rtd07a043.mdb.yandexcloud.net:9091

   # send message
   echo "dev message" | kcat -P -b rc1a-a9585n0rtd07a043.mdb.yandexcloud.net:9091 -t dev-topic -k key -K:

   # read from topic
   kcat -C -b rc1a-a9585n0rtd07a043.mdb.yandexcloud.net:9091 -t dev-topic -q
   ```
   
4. Prepare [python](https://cloud.yandex.ru/docs/managed-kafka/operations/connect#kafka-python) env for Kafka:

   ```bash
   conda create -n kafka-env python=3.8 -y
   conda activate kafka-env
   pip install -r requirements.txt
   ```
   
5. You can run the code for producer and watch the messages in kafkacat:
   ```bash
   python producer.py --data data/green_tripdata_2019-01.csv.gz --color green --topic rides_green
   kcat -C -b rc1a-a9585n0rtd07a043.mdb.yandexcloud.net:9091 -t rides_green -q

   python producer.py --data data/fhv_tripdata_2019-01_head.csv.gz --color fhv --topic rides_fhv
   kcat -C -b rc1a-a9585n0rtd07a043.mdb.yandexcloud.net:9091 -t rides_fhv -q
   ```

### Very useful lectures about Kafka and Kafka Streaming:
* https://youtu.be/4pWAgB4wzhU
* https://youtu.be/QvIM_SJzS5M
* https://youtu.be/FQnPMb0jit0
