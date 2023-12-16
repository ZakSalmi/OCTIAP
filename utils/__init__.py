import os

OTX_API_KEY = os.environ.get("OTX_API_KEY", "b33bf61f62c7e7da86bb84481591a0844630323831ec210fd8c2e2efbfdc131a")

HEADERS = {
    'Content-Type': 'application/json',
    'X-OTX-API-KEY': OTX_API_KEY,
}

KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
KAFKA_GENERAL_TOPIC = 'otx_data_topic'
KAFKA_THREAT_TOPIC = 'Threat_countries'
KAFKA_TARGET_COUNTRIES = 'Target_countries'
KAFKA_THREAT_TYPES = "Threat_types"
GROUP_ID = 'target_filter_group'
