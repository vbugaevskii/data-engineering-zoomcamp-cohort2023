import os
from dotenv import load_dotenv


def get_kafka_config():
    load_dotenv()

    return dict(
        bootstrap_servers=[
            "rc1a-cfsongcosevdstr7.mdb.yandexcloud.net:9091",
            "rc1a-dpcbjr36v0m3hqij.mdb.yandexcloud.net:9091",
            "rc1a-rb5l0smprrcqojlp.mdb.yandexcloud.net:9091",
            "rc1a-up0snrtao9kga6n8.mdb.yandexcloud.net:9091",
        ],
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=os.environ['KAFKA_USER'],
        sasl_plain_password=os.environ['KAFKA_PASS'],
        ssl_cafile=os.environ['KAFKA_CERT'],
    )
