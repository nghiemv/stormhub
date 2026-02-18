"""Runner script to resume a storm catalog and storm collection for SST."""

# standard imports
import os
import sys
import json
import time

# mount the stormhub directory
cur_dir = os.path.dirname(__file__)
storm_dir = os.path.abspath(os.path.join(cur_dir, ".."))
sys.path.append(storm_dir)

# custom imports
from stormhub.logger import initialize_logger
from stormhub.met.storm_catalog import add_storm_dss_files, resume_collection
from stormhub.utils import StacPathManager

def load_config(config_path):
    """Load configuration from a JSON file."""
    with open(config_path, "r") as file:
        return json.load(file)


if __name__ == "__main__":
    start_time = time.time()
    initialize_logger()

    # Catalog Args
    root_dir = "/workspaces/stormhub"
    aoi_name = "allegheny"
    config_params = f"{root_dir}/configs/params-config.json"
    catalog_id = f"{aoi_name}_storms"
    catalog_path = f"{root_dir}/catalogs/{catalog_id}/catalog.json"

    local_directory = f"{root_dir}/catalogs"

    # Unpack config parameters for collection creation
    with open(config_params, "r") as file:
        config = json.load(file)
        params = config.get("params", {})

    # All Collection Args
    start_date = params.get("start_date", "2020-04-01")
    end_date = params.get("end_date", "2020-04-30")
    top_n_events = params.get("top_n_events", 10) # avg per year
    check_every_n_hours = params.get("check_every_n_hours", 24) # storm spacing
    num_workers = params.get("num_workers", 12) # for parallel processing
    storm_duration_hours = params.get("storm_duration_hours", 72) # hours
    min_precip_threshold_inches = params.get("min_precip_threshold_inches", 1.0) # inches
    output_resolution_km = params.get("output_resolution_km", 1)

    # Set up storm catalog file
    spm = StacPathManager(os.path.join(local_directory, catalog_id))
    storm_catalog_file = spm.catalog_file

    # Resume collection with loaded parameters
    storm_collection = resume_collection(
        catalog=storm_catalog_file,
        start_date=start_date,
        end_date=end_date,
        storm_duration=storm_duration_hours,
        min_precip_threshold=min_precip_threshold_inches,
        top_n_events=top_n_events,
        check_every_n_hours=storm_duration_hours,
        use_threads=True,
        create_items=True,
    )

    add_storm_dss_files(storm_catalog_file, output_resolution_km=output_resolution_km)
