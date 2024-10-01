import csv
import json


# Function to read the JSON file and process the data
def process_json(file_path, output_csv):
    # Open the JSON file
    with open(file_path, "r") as file:
        data = json.load(file)  # Load JSON data from the file

    # Open the CSV file for writing
    with open(output_csv, mode="w", newline="") as csv_file:
        # Define the fieldnames (headers) for the CSV
        fieldnames = ["peer_id", "latitude", "longitude", "stake_weight"]

        # Create a writer object
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Loop through each item in the JSON data
        for item in data:
            # Extract the relevant fields
            peer_id = item.get("peer_id")
            latitude = item.get("latitude")
            longitude = item.get("longitude")
            validator_count = item.get("validator_count", 0)
            validator_count_accuracy = item.get("validator_count_accuracy", 1)

            # Calculate stake weight
            stake_weight = validator_count * validator_count_accuracy

            # Write the row to the CSV file
            writer.writerow(
                {"peer_id": peer_id, "latitude": latitude, "longitude": longitude, "stake_weight": stake_weight}
            )

    print(f"Data has been written to {output_csv}")


# Path to your JSON file
json_file_path = "ethereum-validator.json"

# Path to your output CSV file
output_csv_path = "output.csv"

# Call the function to process the JSON file and dump into CSV
process_json(json_file_path, output_csv_path)
