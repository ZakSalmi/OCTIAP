import os

OTX_API_KEY = os.environ.get(
    "OTX_API_KEY", "b33bf61f62c7e7da86bb84481591a0844630323831ec210fd8c2e2efbfdc131a"
)

HEADERS = {
    "Content-Type": "application/json",
    "X-OTX-API-KEY": OTX_API_KEY,
}

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_GENERAL_TOPIC = "otx_data_topic"
KAFKA_THREAT_TOPIC = "threat_countries"
KAFKA_TARGET_COUNTRIES = "target_countries"
KAFKA_THREAT_TYPES = "threat_types"

GENERAL_GROUP_ID = "general_group"
SPECEFIC_COUNTRIES_GROUP_ID = "specefic_group"

GENERAL_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
    "group.id": GENERAL_GROUP_ID,
    "auto.offset.reset": "earliest",
}

SPECEFIC_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
    "group.id": SPECEFIC_COUNTRIES_GROUP_ID,
    "auto.offset.reset": "earliest",
}

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
    "compression.type": "gzip",
}
