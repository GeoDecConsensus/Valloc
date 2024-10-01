import csv
import json


# Function to read the JSON file and process the data into CSV
def json_to_csv(json_file_path, output_csv_path):
    # Open the JSON file and load the data
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)

    # Open the CSV file for writing
    with open(output_csv_path, mode="w", newline="") as csv_file:
        # Define the fieldnames (headers) for the CSV
        fieldnames = ["uuid", "latitude", "longitude", "stake_weight"]

        # Create a CSV writer object
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Loop through each item in the JSON data and extract the relevant fields
        for item in data:
            try:
                moniker = item.get("nodePubkey")  # UUID from moniker
                location = item.get("location", {})
                if location is None:
                    location = {"ll": [0, 0]}
                latitude, longitude = location.get("ll", [0, 0])  # Latitude and Longitude from location.ll
                activated_stake = item.get("activatedStake", 0)  # Stake weight from activatedStake

                # Write the row to the CSV file
                writer.writerow(
                    {"uuid": moniker, "latitude": latitude, "longitude": longitude, "stake_weight": activated_stake}
                )
            except Exception as e:
                print(f"Error processing item: {item}")
                print(f"Error: {e}")

    print(f"Data successfully written to {output_csv_path}")


# Path to your JSON file
json_file_path = "solana.json"

# Path to your output CSV file
output_csv_path = "solana.csv"

# Call the function to convert JSON to CSV
json_to_csv(json_file_path, output_csv_path)
