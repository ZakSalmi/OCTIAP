import json
import sqlite3
import pandas as pd
from OTXv2 import OTXv2
from utils import HEADERS
from utils.get_indicators import get_indicators


def get_table_columns(table_name, cursor):
    # Execute a query to get the column names for a specific table
    cursor.execute(f"PRAGMA table_info({table_name});")

    # Fetch the results
    columns = cursor.fetchall()

    # Return a list of column names
    return [column[1] for column in columns]


def process_data(data, columns):
    # Ensure that the column names are valid keys
    valid_columns = [name.replace(" ", "_").lower() for name in columns]

    # Zip the valid column names with the data
    formated = [dict(zip(valid_columns, row)) for row in data]

    return formated


def get_table(table_name):
    # Pulses Table
    query = f"SELECT * FROM {table_name} order by created"

    cursor.execute(query)

    # Fetch the results
    results = cursor.fetchall()

    # Get the columns
    columns = get_table_columns(table_name, cursor)

    # Dataframe
    result = pd.DataFrame(results, columns=columns)
    return result


def post_data(pulses_data, merged_data):
    try:
        for i, pulse_data in enumerate(pulses_data):
            pulse_name = (
                pulse_data["name"] if len(pulse_data["name"]) >= 5 else "Undefined Name"
            )

            pulse_indicators = merged_data[merged_data["pulse_id"] == pulse_data["id"]][
                [
                    "indicator",
                    "type",
                    "created",
                    "content",
                    "title",
                    "description",
                    "expiration",
                    "is_active",
                ]
            ].to_dict(orient="records")

            if not pulse_indicators:
                print(f"No indicators found for pulse: {pulse_name}")
                continue
            # pprint(pulse_data['id'])

            otx.create_pulse(
                name=pulse_name,
                public=True,
                indicators=pulse_indicators,
                tags=[],
                references=[],
            )
            if i % 100 == 0:
                print(i)

    except Exception:
        pass


if __name__ == "__main__":
    connection = sqlite3.connect("2023-10-25_cti_data_majd.db")
    cursor = connection.cursor()

    otx = OTXv2(HEADERS["X-OTX-API-KEY"])

    pulses_data = get_table("pulses")
    indicators_data = get_table("indicators")
    ip_data = get_table("ip_location")

    merged = pd.merge(
        indicators_data, ip_data, left_on="indicator", right_on="ip", how="outer"
    ).drop("ip", axis=1)

    pulses = otx.getall()

    with open("pulses.json", "w") as file:
        json.dump(pulses, file, indent=6)

    with open("ipv4.json", "w") as file:
        json.dump(get_indicators(pulses, "IPv4"), file, indent=6)

    connection.close()
