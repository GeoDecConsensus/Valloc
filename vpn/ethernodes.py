import csv
import json
import os
import uuid

import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define the base URL for the IP info API
IPINFO_TOKEN = os.getenv("IPINFO_TOKEN")
API_URL = f"https://ipinfo.io/{{}}?token={IPINFO_TOKEN}"

# Get VPN API key from environment variable
VPNAPI_KEY = os.getenv("VPNAPI_KEY")

# Directory to store the output
OUTPUT_DIR = "data/ethernodes"

# Output paths
INPUT_JSON = "data/ethernodes/ethernodes.json"
OUTPUT_CSV = "data/ethernodes/ethernodes_vpn.csv"

# Create the output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# Function to get latitude and longitude from an IP address
def get_lat_long(ip):
    try:
        response = requests.get(API_URL.format(ip))
        if response.status_code == 200:
            data = response.json()
            if "loc" in data:
                latitude, longitude = data["loc"].split(",")
                return latitude, longitude
    except Exception as e:
        print(f"Error fetching data for IP: {ip}, Error: {e}")
    return None, None


# Function to get VPN data for an IP address
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


def process_ethernodes():
    # Load the JSON file
    with open(INPUT_JSON, "r") as json_file:
        json_data = json.load(json_file)

    # Open a new CSV file for writing
    with open(OUTPUT_CSV, mode="w", newline="") as csv_file:
        fieldnames = ["uuid", "latitude", "longitude", "stake_weight", "vpn", "proxy", "tor", "relay"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Process each entry in the JSON file
        for entry in json_data:
            ip = entry.get("Host")
            sync = entry.get("sync")
            if ip == None or sync == "No":
                print("Skipping invalid entry.")
                continue
            else:
                # Get the latitude and longitude for the IP
                latitude, longitude = get_lat_long(ip)

                # Get VPN data for the IP
                vpn_data = get_vpn_data(ip)

                # Generate a random UUID
                unique_id = str(uuid.uuid4())

                print(f"UUID: {unique_id}, Latitude: {latitude}, Longitude: {longitude}, VPN data: {vpn_data}")

                # Write the row in the CSV if latitude and longitude are valid
                if latitude and longitude:
                    writer.writerow(
                        {
                            "uuid": unique_id,
                            "latitude": latitude,
                            "longitude": longitude,
                            "stake_weight": 1,
                            "vpn": vpn_data.get("vpn", False),
                            "proxy": vpn_data.get("proxy", False),
                            "tor": vpn_data.get("tor", False),
                            "relay": vpn_data.get("relay", False),
                        }
                    )

    print("CSV file created successfully.")


def main():
    process_ethernodes()


if __name__ == "__main__":
    main()
