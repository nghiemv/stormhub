"""Updates the metadata json created by extract_dss_metadata.py to include storm center coordinates from stac items by matching the dss rank to the stac item prefix and id."""

import os
import re
import json
import boto3
from io import BytesIO
from dotenv import load_dotenv
import logging
from stormhub.logger import initialize_logger

initialize_logger()
load_dotenv()
s3 = boto3.client("s3")


def extract_rank(filename):
    """Extract rank from filename, removing leading 0's ('_r003' -> '3')."""
    match = re.search(r"_r(\d+)", filename)
    return str(int(match.group(1))) if match else None


def update_dss_metadata_with_coords(json_path, dss_filenames, bucket, stac_prefix):
    """Add storm center coordinates to DSS metadata based on matched STAC items."""
    for dss_filename in dss_filenames:
        rank = extract_rank(dss_filename)
        if not rank:
            logging.error(f"Skipping {dss_filename}: no rank found.")
            continue

        stac_key = f"{stac_prefix}{rank}/{rank}.json"
        logging.info(f"Extracting coordinates from: {stac_key}")
        try:
            obj = s3.get_object(Bucket=bucket, Key=stac_key)
            item = json.load(obj["Body"])
            transform = item.get("properties", {}).get("aorc:transform", {})
            dss_filenames[dss_filename]["storm_center_lon"] = round(transform.get("storm_center_lon", 0), 4)
            dss_filenames[dss_filename]["storm_center_lat"] = round(transform.get("storm_center_lat", 0), 4)
        except Exception as e:
            logging.error(f"Failed to update {dss_filename}: {e}")

    with open(json_path, "w") as f:
        json.dump(dss_filenames, f, indent=2)

    logging.info(f"Updated storm center coordinates in {json_path}.")


def main(json_path, bucket_name, stac_prefix):
    """Add STAC Item coordinates to metadata json."""
    with open(json_path, "r") as f:
        dss_filenames = json.load(f)
    update_dss_metadata_with_coords(json_path, dss_filenames, bucket_name, stac_prefix)


if __name__ == "__main__":
    bucket_name = "trinity-pilot"
    stac_prefix = "stac/prod-support/storms/72hr-events/"
    json_path = "<path_to>/dss_metadata.json"

    main(json_path, bucket_name, stac_prefix)
