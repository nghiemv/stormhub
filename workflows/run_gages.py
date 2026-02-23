"""Runner script to create a gage catalog."""

# standard imports
import os
import sys
import time

import pystac

# mount the stormhub directory
cur_dir = os.path.dirname(__file__)
storm_dir = os.path.abspath(os.path.join(cur_dir, ".."))
sys.path.append(storm_dir)

# custom imports
from stormhub.hydro.usgs.calibration_events import add_calibration_events_to_collection
from stormhub.hydro.usgs.usgs import new_gage_collection, new_gage_catalog
from stormhub.hydro.utils import find_gages_in_watershed
from stormhub.logger import initialize_logger
from stormhub.utils import StacPathManager


if __name__ == "__main__":
    start_time = time.time()
    initialize_logger(json_logging=True)

    catalog_id = "allegheny_gages"
    local_directory = "/workspaces/stormhub/catalogs/allegheny_gages"
    watershed = "/workspaces/stormhub/data/0_source/huc04/allegheny_huc.geojson"

    gage_catalog = new_gage_catalog(
        catalog_id,
        local_directory=local_directory,
        catalog_description="Allegheny Watershed USGS Gage Catalog",
    )

    gage_numbers = find_gages_in_watershed(watershed, 10)
    new_gage_collection(gage_catalog, gage_numbers, local_directory)

    # Set up gage catalog file
    spm = StacPathManager(os.path.join(local_directory, catalog_id))
    gage_catalog_file = spm.catalog_file

    # Optionally add calibration events to the gage collection
    gage_catalog = pystac.read_file("/workspaces/stormhub/catalogs/allegheny_gages/catalog.json")
    add_calibration_events_to_collection(gage_catalog, tolerance_days=7, top_n_years=50)
