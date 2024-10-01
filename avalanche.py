import csv
import json
import os

import pandas as pd
import requests

# IP info API token from env
IPINFO_TOKEN = os.getenv("IPINFO_TOKEN")

# Directory where the validations JSON files are stored
VALIDATIONS = "avalanche_validations.json"

# Output CSV file
CSV_FILE = "avalanche.csv"


def get_ip_location(ip):
    """Fetch latitude and longitude for a given IP address using the ipinfo.io API."""
    url = f"https://ipinfo.io/{ip}?token={IPINFO_TOKEN}"
    try:
        response = requests.get(url)
        data = response.json()

        # Extract latitude and longitude from 'loc' field
        if "loc" in data:
            loc = data["loc"].split(",")
            latitude = float(loc[0])
            longitude = float(loc[1])
            return latitude, longitude
        else:
            return 0.0, 0.0  # Return 0,0 if location info is not available
    except Exception as e:
        print(f"Error fetching location for IP {ip}: {e}")
        return 0.0, 0.0  # Return 0,0 in case of any error


def process_validations():
    """Process validation files and extract required data."""
    all_data = []

    # Loop through all JSON files in the directory
    with open(VALIDATIONS, "r") as file:
        data = json.load(file)

        for item in data.get("items", []):
            node_id = item.get("nodeId", "")
            ip = item.get("node", {}).get("ip", "")
            stake_weight = item.get("stake", {}).get("total", "0")

            # Get latitude and longitude using the IP address
            if ip:
                latitude, longitude = get_ip_location(ip)
            else:
                latitude, longitude = 0.0, 0.0

            print(f"Processed: {node_id}, {latitude}, {longitude}, {stake_weight}")

            # Append the processed data
            all_data.append(
                {"uuid": node_id, "latitude": latitude, "longitude": longitude, "stake_weight": stake_weight}
            )

    return all_data


def save_to_csv(data):
    """Save the processed data to a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(CSV_FILE, index=False)
    print(f"Saved output to {CSV_FILE}")


def main():
    # Process the validations and extract the required data
    data = process_validations()

    # Save the data to CSV file
    save_to_csv(data)


if __name__ == "__main__":
    main()
