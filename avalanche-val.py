import json
import os

import requests

# Base URL for the API
BASE_URL = "https://api.avascan.info"

# Initial API request URL
INITIAL_URL = f"{BASE_URL}/v2/network/mainnet/staking/validations?status=active"

# Directory to store the JSON responses
OUTPUT_DIR = "avalanche-validations"

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
    with open("avalanche_validations.json", "w") as final_file:
        json.dump(final_output, final_file, indent=2)
    print(f"Merged {len(merged_items)} items into 'merged_validations.json'.")


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


if __name__ == "__main__":
    main()
