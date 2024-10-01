import csv
import json


# Function to read the JSON file and process the data into CSV
def json_to_csv(json_file_path, output_csv_path):
    # Open the JSON file and load the data
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)

    # Extract the active validators from the JSON data
    active_validators = data.get("data", {}).get("active_validators", [])

    # Open the CSV file for writing
    with open(output_csv_path, mode="w", newline="") as csv_file:
        # Define the fieldnames (headers) for the CSV
        fieldnames = ["uuid", "latitude", "longitude", "stake_weight"]

        # Create a CSV writer object
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Loop through each validator and extract the required fields
        for validator in active_validators:
            stats_validator = validator.get("stats_validator", {})
            peer_id = validator.get("addr")
            voting_power = validator.get("voting_power", 0)  # Stake weight is voting_power

            if stats_validator is None:
                print(f"Validator {validator.get('addr')} - stats_validator is None")

                # Write the row to the CSV file with latitude and longitude as 0
                writer.writerow({"uuid": peer_id, "latitude": 0, "longitude": 0, "stake_weight": voting_power})
                continue

            latitude = stats_validator.get("location_stats", {}).get("latitude")
            longitude = stats_validator.get("location_stats", {}).get("longitude")

            # Write the row to the CSV file
            writer.writerow(
                {"uuid": peer_id, "latitude": latitude, "longitude": longitude, "stake_weight": voting_power}
            )

    print(f"Data successfully written to {output_csv_path}")


# Path to your JSON file
json_file_path = "aptos.json"

# Path to your output CSV file
output_csv_path = "aptos.csv"

# Call the function to convert JSON to CSV
json_to_csv(json_file_path, output_csv_path)
