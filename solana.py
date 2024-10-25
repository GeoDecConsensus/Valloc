import csv
import json
import os
import time

import requests

# ------------------------------ Constants ------------------------------ #

# **IMPORTANT:** Replace the API_KEY below with your actual API key.
# For enhanced security, consider using environment variables instead of hardcoding.
API_KEY = os.getenv("SOLANA_API_KEY")
BASE_URL = "https://api.solanabeach.io/v1"

HEADERS = {"Accept": "application/json", "Authorization": API_KEY}

# Rate Limiting Parameters
STANDARD_LIMIT = 100  # Maximum number of requests
STANDARD_WINDOW = 10  # Time window in seconds
REQUESTS_MADE = 0
START_TIME = time.time()

# Filenames
VALIDATORS_JSONL = "solana.jsonl"
VALIDATORS_JSON = "solana.json"
CSV_FILE = "solana.csv"
ERROR_LOG = "error_log.txt"

# ------------------------------ Functions ------------------------------ #

def rate_limit():
    """
    Ensures that the script does not exceed the API rate limits.
    If the number of requests made reaches STANDARD_LIMIT within STANDARD_WINDOW,
    the script sleeps for the remaining time in the window.
    """
    global REQUESTS_MADE, START_TIME
    REQUESTS_MADE += 1
    elapsed = time.time() - START_TIME
    if REQUESTS_MADE >= STANDARD_LIMIT:
        if elapsed < STANDARD_WINDOW:
            sleep_time = STANDARD_WINDOW - elapsed
            print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
        # Reset counters
        START_TIME = time.time()
        REQUESTS_MADE = 0


def initialize_files():
    """
    Initializes the JSON Lines, JSON, and error log files if they don't exist.
    Creates empty files to prepare for data appending.
    """
    if not os.path.isfile(VALIDATORS_JSONL):
        open(VALIDATORS_JSONL, "w").close()
        print(f"Created {VALIDATORS_JSONL}.")
    if not os.path.isfile(ERROR_LOG):
        open(ERROR_LOG, "w").close()
        print(f"Created {ERROR_LOG}.")
    if not os.path.isfile(VALIDATORS_JSON):
        open(VALIDATORS_JSON, "w").close()
        print(f"Created {VALIDATORS_JSON}.")


def append_jsonl(filename: str, data: dict):
    """
    Appends a JSON object to a JSON Lines file.
    Each JSON object is written on a separate line.

    Parameters:
        filename (str): The path to the JSON Lines file.
        data (dict): The JSON data to append.
    """
    try:
        with open(filename, "a", encoding="utf-8") as f:
            json.dump(data, f)
            f.write("\n")
        print(f"Appended data to {filename}.")
    except Exception as e:
        error_message = f"Error appending to {filename}: {e}"
        print(error_message)
        log_error(error_message)


def log_error(message: str):
    """
    Logs errors to the ERROR_LOG file and prints them to the console.

    Parameters:
        message (str): The error message to log.
    """
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(message + "\n")
    print(f"Logged error: {message}")


def get_all_validators() -> list:
    """
    Fetches all validators from the /validators/all endpoint.

    Returns:
        list: A list of validator dictionaries.
    """
    url = f"{BASE_URL}/validators/all"
    try:
        response = requests.get(url, headers=HEADERS)
        print(f"Request URL: {url}")
        print(f"Response Status Code: {response.status_code}")
        response.raise_for_status()
        validators = response.json()
        print(f"Fetched {len(validators)} validators.")
        return validators
    except requests.exceptions.RequestException as e:
        error_message = f"Error fetching validators: {e}"
        print(error_message)
        log_error(error_message)
        return []


def get_validator_details(vote_pubkey: str) -> dict:
    """
    Fetches detailed information for a specific validator.

    Parameters:
        vote_pubkey (str): The vote public key of the validator.

    Returns:
        dict: The detailed validator data.
    """
    url = f"{BASE_URL}/validator/{vote_pubkey}"
    try:
        response = requests.get(url, headers=HEADERS)
        print(f"Request URL: {url}")
        print(f"Response Status Code: {response.status_code}")
        response.raise_for_status()
        validator_data = response.json()
        print(f"Successfully fetched details for validator {vote_pubkey}.")
        return validator_data
    except requests.exceptions.RequestException as e:
        error_message = f"Error fetching details for validator {vote_pubkey}: {e}"
        print(error_message)
        log_error(error_message)
        return {}


def process_validator_data(validator_data: dict) -> dict:
    """
    Processes the validator data to retain only the 'validator' field,
    replacing 'delegatingStakeAccounts' with its count.

    Parameters:
        validator_data (dict): The raw validator data fetched from the API.

    Returns:
        dict: The processed validator data.
    """
    try:
        validator = validator_data["validator"].copy()  # Create a shallow copy to avoid modifying the original
        delegating_stakes = validator.get("delegatingStakeAccounts", [])
        if isinstance(delegating_stakes, list):
            validator["delegatingStakeAccounts"] = len(delegating_stakes)
        else:
            validator["delegatingStakeAccounts"] = 0
        return validator
    except KeyError as e:
        error_message = f"Key error during processing validator data: {e}"
        print(error_message)
        log_error(error_message)
        return {}


def json_to_csv(json_file_path, output_csv_path):
    """
    Converts JSON data to CSV format.

    Parameters:
        json_file_path (str): Path to the JSON file.
        output_csv_path (str): Path to the output CSV file.
    """
    try:
        with open(json_file_path, "r") as json_file:
            data = [json.loads(line) for line in json_file]

        with open(output_csv_path, mode="w", newline="") as csv_file:
            fieldnames = ["uuid", "latitude", "longitude", "stake_weight"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for item in data:
                try:
                    moniker = item.get("nodePubkey", "N/A")  # UUID from moniker
                    location = item.get("location", {})
                    if location is None:
                        location = {"ll": [0, 0]}
                    latitude, longitude = location.get("ll", [0, 0])  # Latitude and Longitude from location.ll
                    activated_stake = item.get("activatedStake", 0)  # Stake weight from activatedStake

                    writer.writerow({
                        "uuid": moniker,
                        "latitude": latitude,
                        "longitude": longitude,
                        "stake_weight": activated_stake
                    })
                except Exception as e:
                    print(f"Error processing item: {item}")
                    print(f"Error: {e}")

        print(f"Data successfully written to {output_csv_path}")
    except Exception as e:
        error_message = f"Error converting JSON to CSV: {e}"
        print(error_message)
        log_error(error_message)


# ------------------------------ Main Execution ------------------------------ #

def main():
    """
    Main function to orchestrate the fetching, processing, and dumping of validator data.
    """
    # Initialize files
    initialize_files()

    # Fetch all validators
    validators = get_all_validators()
    if not validators:
        print("No validators found. Exiting.")
        return

    total_validators = len(validators)
    for idx, validator in enumerate(validators, 1):
        vote_pubkey = validator.get("votePubkey")
        moniker = validator.get("moniker", "N/A")
        if not vote_pubkey:
            error_message = f"Validator at index {idx} has no votePubkey. Skipping."
            print(error_message)
            log_error(error_message)
            continue

        print(f"\nFetching details for validator {idx}/{total_validators}: {moniker} ({vote_pubkey})")
        validator_details = get_validator_details(vote_pubkey)
        if not validator_details:
            print(f"No details found for validator {moniker}. Skipping.")
            continue

        # Process the validator data
        processed_validator = process_validator_data(validator_details)
        if processed_validator:
            # Append the processed validator data to the JSON Lines file
            append_jsonl(VALIDATORS_JSONL, processed_validator)

        # Handle rate limiting
        rate_limit()

    print("\nAll data has been processed and saved.")
    print(f"Check {VALIDATORS_JSONL} for the processed JSON responses.")
    if os.path.isfile(ERROR_LOG) and os.path.getsize(ERROR_LOG) > 0:
        print(f"Check {ERROR_LOG} for any errors encountered during the process.")

    # Convert JSON to CSV
    json_to_csv(VALIDATORS_JSONL, CSV_FILE)


if __name__ == "__main__":
    main()