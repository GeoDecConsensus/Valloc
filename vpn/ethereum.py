import csv
import json
import os

import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get VPN API key from environment variable
VPNAPI_KEY = os.getenv("VPNAPI_KEY")

# Directory to store the output
OUTPUT_DIR = "data/ethereum"

# Path to your JSON file
JSON_FILE_PATH = "data/ethereum/ethereum-validator.json"

# Path to your output CSV file
OUTPUT_CSV_PATH = "data/ethereum/ethereum_vpn.csv"

# Create the output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


def get_vpn_data(ip):
    """Get VPN data for a given IP address."""
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


# Function to read the JSON file and process the data
def process_json(file_path, output_csv):
    # Open the JSON file
    with open(file_path, "r") as file:
        data = json.load(file)  # Load JSON data from the file

    # Open the CSV file for writing
    with open(output_csv, mode="w", newline="") as csv_file:
        # Define the fieldnames (headers) for the CSV
        fieldnames = ["peer_id", "ip", "vpn", "proxy", "tor", "relay"]

        # Create a writer object
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Loop through each item in the JSON data
        for item in data:
            # Extract the relevant fields
            peer_id = item.get("peer_id")
            ip = item.get("ip", "")

            # Get VPN data if IP is available
            vpn_data = {}
            if ip:
                vpn_data = get_vpn_data(ip)
                print(f"Processed: {peer_id}, {vpn_data}")
            else:
                print(f"No IP found for {peer_id}")

            # Write the row to the CSV file
            writer.writerow(
                {
                    "peer_id": peer_id,
                    "ip": ip,
                    "vpn": vpn_data.get("vpn", False),
                    "proxy": vpn_data.get("proxy", False),
                    "tor": vpn_data.get("tor", False),
                    "relay": vpn_data.get("relay", False),
                }
            )

    print(f"Data has been written to {output_csv}")


# Call the function to process the JSON file and dump into CSV
def main():
    process_json(JSON_FILE_PATH, OUTPUT_CSV_PATH)


if __name__ == "__main__":
    main()
