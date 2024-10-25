import csv
import json
import os

import pandas as pd
import requests

# Base URL for the API
BASE_URL = "https://api.avascan.info"

# Initial API request URL
INITIAL_URL = f"{BASE_URL}/v2/network/mainnet/staking/validations?status=active"

# Directory to store the JSON responses
OUTPUT_DIR = "avalanche-validations"

# Intermediary validation JSON file
VALIDATIONS_FILE = "avalanche_validations.json"

# Output CSV file
CSV_FILE = "avalanche.csv"

# IP info API token from env
IPINFO_TOKEN = os.getenv("IPINFO_TOKEN")


# Create the output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


def fetch_validations(url):
    """Fetch validations from the given URL and return the JSON response."""
    try:
        response = requests.get(url, headers={"accept": "application/json"})
        response.raise_for_status()  # Will raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def save_to_file(data, filename):
    """Save JSON data to a file."""
    with open(filename, "w") as file:
        json.dump(data, file, indent=2)


def merge_all_files(output_dir):
    """Merge the 'items' from all JSON files into one final JSON file."""
    merged_items = []

    # Loop through all files in the output directory
    for file_name in os.listdir(output_dir):
        file_path = os.path.join(output_dir, file_name)
        with open(file_path, "r") as file:
            data = json.load(file)
            if "items" in data:
                merged_items.extend(data["items"])

    # Save the merged data into a final JSON file
    final_output = {"items": merged_items}
    with open(VALIDATIONS_FILE, "w") as final_file:
        json.dump(final_output, final_file, indent=2)
    print(f"Merged {len(merged_items)} items into '{VALIDATIONS_FILE}'.")


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
    with open(VALIDATIONS_FILE, "r") as file:
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
    # Start with the initial URL
    next_url = INITIAL_URL
    page_num = 1

    while next_url:
        print(f"Fetching page {page_num}...")
        data = fetch_validations(next_url)

        if data is None:
            print("Stopping due to an error.")
            break

        # Save the current page's data to a file
        file_name = os.path.join(OUTPUT_DIR, f"validations_page_{page_num}.json")
        save_to_file(data, file_name)
        print(f"Saved page {page_num} data to {file_name}.")

        # Check if there's a next page
        next_link = data.get("link", {}).get("next")
        if next_link:
            next_url = BASE_URL + next_link
            page_num += 1
        else:
            # No more pages
            next_url = None
            print("No more pages to fetch.")

    # Merge all the JSON files into one
    merge_all_files(OUTPUT_DIR)

    # Process the validations and extract the required data
    data = process_validations()

    # Save the data to CSV file
    save_to_csv(data)


if __name__ == "__main__":
    main()