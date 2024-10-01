import http.client
import json
import ssl

import pandas as pd
import requests


class Aptos:
    MAINNET_VALIDATORS_DATA_URL = (
        "https://storage.googleapis.com/aptos-mainnet/explorer/validator_stats_v2.json?cache-version=0"
    )
    MAINNET_FULLNODE_DATA_URL = "fullnode.mainnet.aptoslabs.com"

    @classmethod
    def get_validators(cls):
        print("Retrieving data for Aptos")
        response = requests.get(cls.MAINNET_VALIDATORS_DATA_URL)

        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code}")
            return None

        validators_data = response.json()
        validator_info_list = []

        for validator in validators_data:
            owner_address = validator.get("owner_address", "Unknown")
            uuid = owner_address

            location_stats = validator.get("location_stats", {})
            latitude = location_stats.get("latitude", 0)
            longitude = location_stats.get("longitude", 0)

            tokens = cls.get_tokens(owner_address)
            stake_weight = tokens

            validator_info = {"uuid": uuid, "latitude": latitude, "longitude": longitude, "stake_weight": stake_weight}

            print(validator_info)
            validator_info_list.append(validator_info)

        df = pd.DataFrame(validator_info_list)
        df.to_csv("aptos-2.csv", index=False)
        print("CSV file aptos-2.csv has been written.")
        return df

    @classmethod
    def get_tokens(cls, address):
        conn = http.client.HTTPSConnection(cls.MAINNET_FULLNODE_DATA_URL, context=ssl._create_unverified_context())
        headers = {"Accept": "application/json"}
        request_path = f"/v1/accounts/{address}/resource/0x1::stake::StakePool"
        try:
            conn.request("GET", request_path, headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            if res.status != 200:
                print(f"Failed to retrieve tokens for address {address}: {res.status}")
                return 0
            json_object = json.loads(data)

            active_value = json_object.get("data", {}).get("active", {}).get("value")
            if active_value is not None:
                return int(active_value)
            else:
                print(f"Active value not found in response for address {address}")
                return 0
        except Exception as e:
            print(f"Exception occurred while retrieving tokens for address {address}: {e}")
            return 0
        finally:
            conn.close()


if __name__ == "__main__":
    Aptos.get_validators()
