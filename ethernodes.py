import csv
import json
import os
import uuid

import requests

# Define the base URL for the IP info API
IPINFO_TOKEN = os.getenv("IPINFO_TOKEN")
API_URL = f"https://ipinfo.io/{{}}?token={IPINFO_TOKEN}"


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


# Load the JSON file
with open("ethernodes.json", "r") as json_file:
    json_data = json.load(json_file)

# Open a new CSV file for writing
with open("ethernodes.csv", mode="w", newline="") as csv_file:
    fieldnames = ["uuid", "latitude", "longitude", "stake_weight"]
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

            # Generate a random UUID
            unique_id = str(uuid.uuid4())

            print(f"UUID: {unique_id}, Latitude: {latitude}, Longitude: {longitude}")

            # Write the row in the CSV if latitude and longitude are valid
            if latitude and longitude:
                writer.writerow({"uuid": unique_id, "latitude": latitude, "longitude": longitude, "stake_weight": 1})

print("CSV file created successfully.")
