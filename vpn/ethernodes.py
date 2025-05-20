import csv
import json
import os

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
    """Get latitude and longitude for a given IP address."""
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


def get_vpn_data(ip):
    """Get VPN data for a given IP address."""
    try:
        url = f"https://vpnapi.io/api/{ip}?key={VPNAPI_KEY}"
        response = requests.get(url)
        data = response.json()

        # Check if there's an error message in the response
        if "message" in data:
            print(f"Error for IP {ip}: {data['message']}")
            return {}, None, None

        # Extract location data if available
        location_data = data.get("location", {})
        latitude = location_data.get("latitude")
        longitude = location_data.get("longitude")

        # Check if security field exists in the response
        security_data = {}
        if "security" in data:
            security_data = data["security"]
        else:
            print(f"No security data for IP {ip}")

        return security_data, latitude, longitude
    except Exception as e:
        print(f"Error fetching VPN data for IP {ip}: {str(e)}")
        return {}, None, None


def process_ethernodes():
    """Process the ethernodes data and save to CSV."""
    # Get existing node IDs from CSV file if it exists
    existing_ids = set()
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                existing_ids.add(row["id"])
        print(f"Found {len(existing_ids)} existing node IDs in the CSV")

    # Load the JSON file
    with open(INPUT_JSON, "r") as json_file:
        json_data = json.load(json_file)

    # Open the CSV file for appending if it exists, or writing if it doesn't
    mode = "a" if os.path.exists(OUTPUT_CSV) and existing_ids else "w"
    with open(OUTPUT_CSV, mode=mode, newline="") as csv_file:
        # Define the fieldnames for the CSV
        fieldnames = ["id", "latitude", "longitude", "stake_weight", "vpn", "proxy", "tor", "relay"]

        # Create a writer object
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header only if we're creating a new file
        if mode == "w":
            writer.writeheader()

        # Process each entry in the JSON file
        skipped_count = 0
        processed_count = 0
        failed_count = 0
        for entry in json_data:
            node_id = entry.get("Id")
            ip = entry.get("Host")
            sync = entry.get("sync")

            # Skip if entry is invalid or already processed
            if ip is None or sync == "No":
                print(f"Skipping invalid entry: {entry}")
                continue

            if node_id in existing_ids:
                print(f"Skipping already processed ID: {node_id}")
                skipped_count += 1
                continue

            # Get VPN and location data
            vpn_data, api_latitude, api_longitude = get_vpn_data(ip)

            # Use location data from API if available, otherwise try to get it from ipinfo
            latitude, longitude = api_latitude, api_longitude
            if not (latitude and longitude):
                latitude, longitude = get_lat_long(ip)

            # Only add to CSV if we successfully retrieved the location data and VPN data
            if latitude and longitude and vpn_data:
                print(f"Processed: {node_id}, Lat: {latitude}, Long: {longitude}, VPN data: {vpn_data}")
                processed_count += 1

                # Write the row to the CSV file
                writer.writerow(
                    {
                        "id": node_id,
                        "latitude": latitude,
                        "longitude": longitude,
                        "stake_weight": 1,
                        "vpn": vpn_data.get("vpn", False),
                        "proxy": vpn_data.get("proxy", False),
                        "tor": vpn_data.get("tor", False),
                        "relay": vpn_data.get("relay", False),
                    }
                )
            else:
                print(f"Failed to get complete data for {node_id} with IP {ip}")
                failed_count += 1

    print(f"Skipped {skipped_count} node IDs that already had VPN data")
    print(f"Processed {processed_count} new node IDs")
    print(f"Failed to process {failed_count} node IDs due to missing data")
    print(f"Data has been written to {OUTPUT_CSV}")


def main():
    process_ethernodes()


if __name__ == "__main__":
    main()
