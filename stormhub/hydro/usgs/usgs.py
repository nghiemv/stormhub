"""Module for createing USGS STAC objects."""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

import geopandas as gpd
from shapely.geometry import shape
from dataretrieval import nwis, NoSitesError
from pystac import Asset, Item, MediaType, RelType, Link, Collection
import pystac
from stormhub.hydro.utils import log_pearson_iii
import xarray as xr
import pandas as pd
import shapely


from stormhub.hydro.plots import (
    plot_ams,
    plot_ams_seasonal,
    plot_log_pearson_iii,
    plot_nwis_statistics,
)
from stormhub.utils import file_table


def prepare_swe_data(
    swe_dataarray: xr.DataArray,
    start_date: datetime,
    end_date: datetime,
    crs: str = "EPSG:4326",
    x_dim: str = "lon",
    y_dim: str = "lat",
):
    """Slices time and sets spatial dimensions/CRS."""
    swe_subset = swe_dataarray.sel(time=slice(start_date, end_date))
    if swe_subset is None or swe_subset.size == 0:
        raise ValueError("No SWE data found for the specified date range.")
    swe_subset.rio.write_crs(crs, inplace=True)
    swe_subset.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim, inplace=True)
    return swe_subset


def clip_swe_to_geometry(swe_dataarray: xr.DataArray, geometry: shapely.geometry.Polygon, crs: str):
    """Clips the DataArray to a specific geometry."""
    return swe_dataarray.rio.clip([geometry], crs)


def calculate_spatial_mean(clipped_da: xr.DataArray, x_dim: str = "lon", y_dim: str = "lat"):
    """Calculate spatial mean and formats to DataFrame."""
    daily_mean = clipped_da.mean(dim=[y_dim, x_dim])
    df = daily_mean.to_dataframe().reset_index()
    return df[["time", "SWE"]].rename(columns={"SWE": "daily_mean_swe_mm"})


def avg_daily_swe(
    geometry: shapely.geometry.Polygon,
    swe_dataarray: xr.DataArray,
    start_date: datetime,
    end_date: datetime,
    crs: str = "EPSG:4326",
    da_xdim: str = "lon",
    da_ydim: str = "lat",
):
    """Calculate daily average SWE over a specified geometry and time range.

    Args:
        geometry (shapely.geometry.Polygon): The geometry to clip the SWE data.
        swe_dataarray (xr.DataArray): The SWE data array.
        start_date (datetime): Start date for the time range.
        end_date (datetime): End date for the time range.
        crs (str): Coordinate reference system of the SWE data.
        da_xdim (str): Name of the x dimension in the DataArray.
        da_ydim (str): Name of the y dimension in the DataArray.

    Returns
    -------
        pd.DataFrame: DataFrame with daily average SWE values.
    """
    # Prepare Data
    prepared_da = prepare_swe_data(swe_dataarray, start_date, end_date, crs, da_xdim, da_ydim)

    # Clip to geom
    clipped_da = clip_swe_to_geometry(prepared_da, geometry, crs)

    # Aggregate
    final_df = calculate_spatial_mean(clipped_da, da_xdim, da_ydim)

    return final_df


def add_ams_swe_to_gage_collection(
    gage_collection: Collection,
    drainage_area_geojson_path: str,
    swe_zarr_path: str,
    swe_variable_name: str = "SWE",
    days_in_event: int = 30,
):
    """Add average SWE assets to gage items in the given collection. Uses AMS parquet file for each gage to determine event dates.

    Args:
        gage_collection (Collection): STAC Collection of gage items.
        drainage_area_geojson_path (str): Path to GeoJSON file with drainage area polygons. Each gage feature should have a "name" property matching the gage number and a matching drainage areas geometry.
        swe_zarr_path (str): Local or S3 path to the SWE Zarr dataset.
        swe_variable_name (str): Name of the SWE variable in the dataset.
        days_in_event (int): Number of days to look back for each AMS event.

    Returns
    -------
        None
    """
    snow_ds = xr.open_zarr(swe_zarr_path, consolidated=True)
    swe_da = snow_ds[swe_variable_name]

    da_polygons = gpd.read_file(drainage_area_geojson_path)

    for item in gage_collection.get_all_items():
        try:
            gage_number = item.id
            item_href = item.get_self_href()
            item_dir = item_href.rpartition("/")[0]

            ams_href = None
            for asset in item.get_assets().values():
                if asset.href.endswith("ams.pq"):
                    ams_href = asset.href
                    break

            if ams_href is None:
                logging.warning(f"No AMS asset found for gage {gage_number}, skipping.")
                continue

            ams_path = f"{item_dir}/{ams_href.split('/')[-1]}"
            logging.info(f"Processing Gage: {gage_number} from {ams_path}")
            if os.path.exists(ams_path):
                ams_pq = pd.read_parquet(ams_path)
            else:
                logging.warning(f"AMS file {ams_path} does not exist, skipping gage {gage_number}.")
                continue

            single_gage_da = da_polygons[da_polygons["Name"] == gage_number]
            drainage_area = single_gage_da.geometry.values[0]

            ams_dates = ams_pq.index
            swe_results = []

            for date in ams_dates:
                logging.info(f"Processing SWE for date: {date}")
                end_date = date.tz_localize(None)
                start_date = end_date - timedelta(days=days_in_event)

                try:
                    daily_swe = avg_daily_swe(drainage_area, swe_da, start_date=start_date, end_date=end_date)

                    daily_swe["ams_ref_date"] = end_date

                    swe_results.append(daily_swe)
                except Exception as e:
                    logging.error(f"Skipping {end_date}: {e}")

            if len(swe_results) == 0:
                logging.warning(f"No SWE results for gage {gage_number}, skipping asset addition.")
                continue

            combined_swe_df = pd.concat(swe_results, ignore_index=True)
            swe_fn = f"avg_ams_swe_{gage_number}.pq"
            combined_swe_df.to_parquet(f"{item_dir}/{swe_fn}")

            item.add_asset(
                "avg_ams_swe",
                Asset(
                    href=f"{swe_fn}",
                    media_type="application/parquet",
                    roles=["data"],
                    title="Average SWE",
                    description="Average daily SWE over gage drainage area per AMS Event",
                ),
            )
            item.save_object()
        except Exception as e:
            logging.error(f"Error processing gage {gage_number}: {e}")


class UsgsGage(Item):
    """A class representing a USGS gage as a STAC item."""

    @classmethod
    def from_usgs(cls, gage_number: str, href: Optional[str] = None, **kwargs):
        """Create a STAC Item representing a USGS stream gage.

        Parameters
        ----------
            gage_number (str): USGS gage number.
            href (Optional[str]): Item href for the created USGS gage item. Optional

        Returns
        -------
            pystac.Item: A STAC Item representing the USGS gage.
        """
        if href is None:
            href = f"{gage_number}.json"

        site_data = cls._load_site_data(gage_number)

        geometry = {"type": "Point", "coordinates": [site_data["dec_long_va"], site_data["dec_lat_va"]]}
        bbox = [
            site_data["dec_long_va"],
            site_data["dec_lat_va"],
            site_data["dec_long_va"],
            site_data["dec_lat_va"],
        ]
        properties = {
            "site_no": site_data["site_no"],
            "station_nm": site_data["station_nm"],
            "huc_cd": str(site_data["huc_cd"]),
            "drain_area_va": float(site_data["drain_area_va"]),
            "daily_values": {
                "begin_date": site_data["daily_values"]["begin_date"],
                "end_date": site_data["daily_values"]["end_date"],
            },
            "site_retrieved": site_data["site_retrieved"],
        }
        start_datetime, end_datetime = cls.start_end_dates(gage_number)
        if properties["daily_values"]["begin_date"] is None and properties["daily_values"]["end_date"] is None:
            properties["daily_values"]["end_date"] = end_datetime.strftime("%Y-%m-%d") if end_datetime else None
            properties["daily_values"]["begin_date"] = start_datetime.strftime("%Y-%m-%d") if start_datetime else None

        logging.info(f"Creating UsgsGage {gage_number} {site_data['station_nm']}")

        usgs_gage = cls(
            id=gage_number,
            geometry=geometry,
            bbox=bbox,
            datetime=datetime.now(),
            properties=properties,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            href=href,
            **kwargs,
        )

        gage_url = f"https://waterdata.usgs.gov/nwis/inventory/?site_no={properties['site_no']}"
        usgs_gage.add_link(Link(rel=RelType.VIA, target=gage_url, title="USGS NWIS Site Information"))
        return usgs_gage

    def __repr__(self):
        """Return string representation of the UsgsGage object."""
        return f"<UsgsGage {self.id} {self.properties['station_nm']}>"

    @staticmethod
    def _load_site_data(gage_number: str) -> dict:
        """Query NWIS for site information."""
        resp = nwis.get_record(sites=gage_number, service="site")

        return {
            "site_no": resp["site_no"].iloc[0],
            "station_nm": resp["station_nm"].iloc[0],
            "dec_lat_va": float(resp["dec_lat_va"].iloc[0]),
            "dec_long_va": float(resp["dec_long_va"].iloc[0]),
            "drain_area_va": resp["drain_area_va"].iloc[0],
            "huc_cd": resp["huc_cd"].iloc[0],
            "alt_datum_cd": resp["alt_datum_cd"].iloc[0],
            "site_retrieved": datetime.now().isoformat(),
            "daily_values": {
                "begin_date": resp["begin_date"].iloc[0] if "begin_date" in resp else None,
                "end_date": resp["end_date"].iloc[0] if "end_date" in resp else None,
            },
        }

    def start_end_dates(gage_id: str):
        """Retrieve start and end dates from oldest and newest daily value records."""
        startDate = "1900-01-01"
        endDate = datetime.now().strftime("%Y-%m-%d")
        dv = nwis.get_dv(gage_id, startDate, endDate)[0]
        dv_sorted = dv.sort_index()
        if len(dv_sorted) > 1:
            return dv_sorted.index.min().to_pydatetime(), dv_sorted.index.max().to_pydatetime()
        else:
            return None, None

    def get_peaks(self, item_dir: str, make_plots: bool = True):
        """Retrieve annual maximum series from NWIS and make the plots associated with those values."""
        gage_id = self.properties["site_no"]

        try:
            df = nwis.get_record(service="peaks", sites=[gage_id])
        except NoSitesError:
            logging.warning(f"Peaks could not be found for gage id: {gage_id}")
            return

        file_name = os.path.join(item_dir, f"{gage_id}-ams.pq")

        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        df.to_parquet(file_name)

        try:
            peaks = log_pearson_iii(df["peak_va"])
        except ValueError:
            logging.warning(f"LP3 peaks stats could not be calculated for gage id: {gage_id}")
            return

        asset = Asset(
            file_name,
            title="Annual Maximum Series Parquet",
            media_type=MediaType.PARQUET,
            roles=["data"],
            extra_fields={"file:values": file_table(peaks, "return_period", "discharge_CFS_(Approximate)")},
        )

        self.add_asset("annual_maxima_series", asset)

        if make_plots:
            # AMS Plot 1
            filename = os.path.join(item_dir, f"{gage_id}-ams.png")
            plot_ams(df, gage_id, filename)

            asset = Asset(filename, title="AMS Plot", media_type=MediaType.PNG, roles=["thumbnail"])
            self.add_asset("ams_plot", asset)

            # AMS Plot 2
            filename = os.path.join(item_dir, f"{gage_id}-ams-seasonal.png")
            plot_ams_seasonal(df, gage_id, filename)

            asset = Asset(filename, title="AMS Seasonal Plot", media_type=MediaType.PNG, roles=["thumbnail"])
            self.add_asset("ams_seasons_plot", asset)

            # LPII Plot
            filename = os.path.join(item_dir, f"{gage_id}-ams-lpiii.png")
            plot_log_pearson_iii(df["peak_va"], gage_id, filename)

            asset = Asset(filename, title="AMS LPIII Plot", media_type=MediaType.PNG, roles=["thumbnail"])
            self.add_asset("ams_LPIII_plot", asset)

    def get_flow_stats(self, item_dir: str, make_plots: bool = True):
        """Retrieve and plot day of the year flow statistics."""
        gage_id = self.properties["site_no"]

        try:
            df = nwis.get_stats(sites=gage_id, parameterCd="00060")[0]
        except IndexError:
            logging.warning(f"Flow stats could not be found for gage_id: {gage_id}")
            return

        file_name = os.path.join(item_dir, f"{gage_id}-flow-stats.pq")

        if not os.path.exists(item_dir):
            os.makedirs(item_dir)

        df.to_parquet(file_name)

        asset = Asset(file_name, title="Flow Statistics Parquet", media_type=MediaType.PARQUET, roles=["data"])

        self.add_asset("flow_stats", asset)

        if make_plots:
            # AMS Plot 1
            filename = os.path.join(item_dir, f"{gage_id}-flow-stats.png")
            plot_nwis_statistics(df, gage_id, filename)

            asset = Asset(filename, title="Flow Statistics Plot", media_type=MediaType.PNG, roles=["thumbnail"])
            self.add_asset("flow_statistics_plot", asset)


def from_stac(href: str) -> UsgsGage:
    """Create a UsgsGage from a STAC Item."""
    return UsgsGage.from_file(href)


class GageCollection(pystac.Collection):
    """USGS gage collection."""

    def __init__(self, collection_id: str, items: List[pystac.Item], href):
        """
        Initialize a GageCollection instance.

        Parameters
        ----------
            collection_id (str): The ID of the collection.
            items (List[pystac.Item]): List of STAC items to include in the collection.
        """
        spatial_extents = [item.bbox for item in items if item.bbox]
        temporal_extents = [item.datetime for item in items if item.datetime is not None]

        collection_extent = pystac.Extent(
            spatial=pystac.SpatialExtent(
                bboxes=[
                    [
                        min(b[0] for b in spatial_extents),
                        min(b[1] for b in spatial_extents),
                        max(b[2] for b in spatial_extents),
                        max(b[3] for b in spatial_extents),
                    ]
                ]
            ),
            temporal=pystac.TemporalExtent(intervals=[[min(temporal_extents), max(temporal_extents)]]),
        )

        super().__init__(
            id=collection_id,
            description="STAC collection generated from gage items.",
            extent=collection_extent,
            href=href,
        )

        for item in items:
            self.add_item_to_collection(item)

    def add_item_to_collection(self, item: Item, override: bool = False):
        """
        Add an item to the collection.

        Parameters
        ----------
            item (Item): The STAC item to add.
            override (bool): Whether to override an existing item with the same ID.
        """
        existing_ids = {item.id for item in self.get_all_items()}

        if item.id in existing_ids:
            if override:
                self.remove_item(item.id)
                item.set_parent(self)
                self.add_item(item)
                logging.info(f"Overwriting (existing) item with ID '{item.id}'.")
            else:
                logging.error(
                    f"Item with ID '{item.id}' already exists in the collection. Use `override=True` to overwrite."
                )
        else:
            item.set_parent(self)
            self.add_item(item)
            logging.info(f"Added item with ID '{item.id}' to the collection.")

    def items_to_geojson(self, items: List[pystac.Item], geojson_dir: str):
        """Add a list of STAC items to a geojson and saves it as a collection asset."""
        records = []
        for item in items:
            geom = shape(item.geometry)
            records.append({"site_no": item.properties.get("site_no"), "geometry": geom})

        gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
        geojson_path = geojson_dir.joinpath("gages.geojson")
        gdf.to_file(geojson_path, driver="GeoJSON")

        geojson_asset = pystac.Asset(
            href=str(geojson_path.relative_to(geojson_dir)).replace("\\", "/"),
            media_type=pystac.MediaType.GEOJSON,
            title="Gages GeoJSON",
        )
        self.add_asset("geojson", geojson_asset)


def new_gage_catalog(catalog_id: str, local_directory: str, catalog_description: str) -> pystac.Catalog:
    """
    Create a new STAC catalog for storing USGS gage collection.

    Parameters
    ----------
        catalog_id (str): Unique id for the STAC catalog.
        local_directory (Optional[str]): Directory where the catalog will be saved.
        catalog_description (str): The description of the catalog.

    Returns
    -------
        pystac.Catalog: The created STAC catalog.
    """
    if not local_directory:
        local_directory = os.getcwd()

    catalog = pystac.Catalog(id=catalog_id, description=catalog_description)
    catalog.normalize_and_save(root_href=local_directory, catalog_type=pystac.CatalogType.SELF_CONTAINED)
    return catalog


def new_gage_collection(catalog: pystac.Catalog, gage_numbers: List[str], directory: str) -> None:
    """
    Create a new STAC collection for USGS gages and adds it to an existing catalog.

    Parameters
    ----------
        catalog (pystac.Catalog): The STAC catalog which the collection will be added to.
        gage_numbers (List[str]): A list of USGS gage site numbers to add to the collection.
        directory (str): The directory where the STAC collection and items will be stored.
    """
    base_dir = Path(directory)
    gages_dir = base_dir.joinpath("gages")
    gages_dir.mkdir(parents=True, exist_ok=True)
    collection_href = base_dir.joinpath("collection.json")

    items = []
    for gage_number in gage_numbers:
        try:
            gage_item_dir = gages_dir.joinpath(gage_number)
            gage_item_dir.mkdir(parents=True, exist_ok=True)

            gage = UsgsGage.from_usgs(gage_number, href=str(gage_item_dir.joinpath(f"{gage_number}.json")))
            gage.get_flow_stats(str(gage_item_dir))
            gage.get_peaks(str(gage_item_dir))

            for asset in gage.assets.values():
                asset.href = os.path.relpath(asset.href, gage_item_dir).replace("\\", "/")

            gage.save_object()
            items.append(gage)
        except Exception as e:
            logging.error(f"Gage {gage_number} failed eith exception: {e}")

    collection = GageCollection("gages", items, str(collection_href))
    collection.items_to_geojson(items, gages_dir)

    catalog.add_child(collection)
    catalog.normalize_and_save(root_href=str(base_dir), catalog_type=pystac.CatalogType.SELF_CONTAINED)

    return collection
