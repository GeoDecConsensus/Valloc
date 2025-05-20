# Script to update the Ethereum validator file with the new IP addresses
# This is used to add the IP addresses to ethereum-validator.json from the ethereum-validator-new.json

import json
import os


def update_validators():
    # File paths
    base_path = os.path.join("data", "ethereum")
    original_file = os.path.join(base_path, "ethereum-validator.json")
    new_file = os.path.join(base_path, "ethereum-validator-new.json")

    print(f"Loading original file: {original_file}")
    with open(original_file, "r") as f:
        original_data = json.load(f)

    print(f"Loading new file: {new_file}")
    with open(new_file, "r") as f:
        new_data = json.load(f)

    # Create a dictionary of peer_id -> ip from the new file
    peer_id_to_ip = {}
    for item in new_data:
        if "peer_id" in item and "ip" in item:
            peer_id_to_ip[item["peer_id"]] = item["ip"]

    print(f"Found {len(peer_id_to_ip)} peer_id to IP mappings in new file")

    # Update the original data
    updates_count = 0
    missing_ip_count = 0
    for item in original_data:
        if "peer_id" in item:
            if item["peer_id"] in peer_id_to_ip:
                item["ip"] = peer_id_to_ip[item["peer_id"]]
                updates_count += 1
            else:
                missing_ip_count += 1

    print(f"Updated {updates_count} entries in original file")
    print(f"Missing IP addresses for {missing_ip_count} peer IDs in original file")

    # Save the updated data back to the original file
    print(f"Saving updated data back to: {original_file}")
    with open(original_file, "w") as f:
        json.dump(original_data, f, indent=2)

    print("Update completed successfully!")


if __name__ == "__main__":
    update_validators()
