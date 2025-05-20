import csv
import json
import os

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Directory to store the JSON responses
OUTPUT_DIR = "data/avalanche"

# Intermediary validation JSON file
VALIDATIONS_FILE = "data/avalanche/avalanche_validations.json"

# Output CSV file
CSV_FILE = "data/avalanche/avalanche_vpn.csv"

VPNAPI_KEY = os.getenv("VPNAPI_KEY")


# Create the output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


def get_vpn_data(ip):
    """Get VPN data for a given IP address."""
    # https://vpnapi.io/api/8.8.8.8?key=YOUR_API_KEY
    try:
        url = f"https://vpnapi.io/api/{ip}?key={VPNAPI_KEY}"
        response = requests.get(url)
        data = response.json()

        # Check if there's an error message in the response
        if "message" in data:
            print(f"Error for IP {ip}: {data['message']}")
            return {}

        # Check if security field exists in the response
        if "security" in data:
            return data["security"]
        else:
            print(f"No security data for IP {ip}")
            return {}
    except Exception as e:
        print(f"Error fetching VPN data for IP {ip}: {str(e)}")
        return {}


def process_validations():
    """Process validation files and extract required data."""
    # Define the fieldnames (headers) for the CSV
    fieldnames = ["uuid", "vpn", "proxy", "tor", "relay"]

    # Create the CSV file and write header
    with open(CSV_FILE, mode="w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        # Loop through all JSON files in the directory
        with open(VALIDATIONS_FILE, "r") as file:
            data = json.load(file)

            for item in data.get("items", []):
                node_id = item.get("nodeId", "")
                ip = item.get("node", {}).get("ip", "")

                if ip:
                    vpn_data = get_vpn_data(ip)
                else:
                    vpn_data = {}

                print(f"Processed: {node_id}, {vpn_data}")

                # Write each row to the CSV file immediately
                writer.writerow(
                    {
                        "uuid": node_id,
                        "vpn": vpn_data.get("vpn", False),
                        "proxy": vpn_data.get("proxy", False),
                        "tor": vpn_data.get("tor", False),
                        "relay": vpn_data.get("relay", False),
                    }
                )

    print(f"Data has been written to {CSV_FILE}")


def main():
    process_validations()


if __name__ == "__main__":
    main()
