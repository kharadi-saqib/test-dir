from eodag import setup_logging
from SatProductCurator.models.constants import (
    PRODUCT_LANDSAT8,
    PRODUCT_LANDSAT9,
    PRODUCT_SENTINEL1,
    PRODUCT_SENTINEL2,
    PRODUCT_SENTINEL3,
    PROVIDER_PEPS,
)
from SatProductCurator.models import SatelliteProviderConfiguration, SatelliteImageTile
import tempfile
import json
import os
import traceback
from typing import List
import rasterio
from eodag import EODataAccessGateway
import datetime
from datetime import date
import geojson
from django.contrib.gis.geos import Polygon
from django.utils import timezone
import uuid
from django.conf import settings
import logging
from IngestionEngine.models import SourceData
from IngestionEngine.workers._base_logger import Logger

log = Logger("SentinelImageTileService").get_logger()


setup_logging(verbose=3)


class SentinelTileService:
    def __init__(self) -> None:
        config = SatelliteProviderConfiguration.objects.get(
            SATProviderName=PROVIDER_PEPS
        )
        print("---------Selected SATProviderName Configuration-----------")
        print("Config Object:", config)
        print("Provider Name:", config.SATProviderName)
        print("Username:", config.Username)
        print("Password:", config.Password)
        os.environ["EODAG__PEPS__AUTH__CREDENTIALS__USERNAME"] = config.Username or ""
        os.environ["EODAG__PEPS__AUTH__CREDENTIALS__PASSWORD"] = config.Password or ""

    def search_by_polygon(
        self, product: str, start_date: date, end_date: date, polygon: Polygon
    ):
        # Instantiate EODataAccessGateway object
        dag = EODataAccessGateway()

        # Determine the productType based on the input product
        if product == PRODUCT_SENTINEL1:
            productType = "S1_SAR_RAW"
        elif product == PRODUCT_SENTINEL2:
            productType = "S2_MSI_L1C"
        elif product == PRODUCT_SENTINEL3:
            productType = "S3_EFR"
        else:
            raise Exception(f"Unknown product: {product}")

        # min_x, min_y, max_x, max_y
        # Get the bounding box (extent) of the polygon
        extent = polygon.envelope.extent

        # Perform search using EODataAccessGateway
        search_results, total_count = dag.search(
            productType=productType,
            start=str(start_date),
            end=str(end_date),
            geom={
                "lonmin": extent[0],
                "latmin": extent[1],
                "lonmax": extent[2],
                "latmax": extent[3],
            },
        )

        # Convert search results to GeoJSON format
        return json.loads(geojson.dumps(search_results))

    def filter_results(self, search_results):
        pass

    def update_to_database(
        self, search_results: dict, product, new_collection_id
    ) -> List[SatelliteImageTile]:
        # Initialize an empty list to store SatelliteImageTile objects
        tiles = []

        # Iterate through each tile JSON object in the search results
        for tile_json in search_results["features"]:
            """try:
                    # Attempt to retrieve the SatelliteImageTile from the database
                    tile = SatelliteImageTile.objects.get(tile_id=tile_json["id"])
                except SatelliteImageTile.DoesNotExist:
                    # Extracting the date from the tile properties and formatting it

                    tile_date = datetime.datetime.strptime(
                        tile_json["properties"]["startTimeFromAscendingNode"],
                        "%Y-%m-%dT%H:%M:%S.%fZ",
                    ).date()

                    # If the tile does not exist in the database, create a new SatelliteImageTile object
                    tile = SatelliteImageTile(
                        Product=product,
                        tile_id=tile_json["id"],
                        date=tile_date,
                        boundary=Polygon(tile_json["geometry"]["coordinates"][0][0]),
                        to_be_downloaded=settings.TO_BE_DOWNLOADED,
                        eodag_data=tile_json,
                        New_Collection_ID=new_collection_id,
                    )
                    tile.save()
                    log(
                        f"Mna New Collection id is: {new_collection_id}.",
                        sat_image_tile=tile,
                        level=logging.DEBUG,
                    )
                    log(
                        f"Mna New Collection id from satellite tile table is: {tile.New_Collection_ID}.",
                        sat_image_tile=tile,
                        level=logging.DEBUG,
                    )

                    log(
                        "Sentinel image tile created Successfully.",
                        sat_image_tile=tile,
                        level=logging.DEBUG,
                    )
                # Add the SatelliteImageTile object to the list of tiles
                tiles.append(tile)

            return tiles"""
            tile_date = datetime.datetime.strptime(
                tile_json["properties"]["startTimeFromAscendingNode"],
                "%Y-%m-%dT%H:%M:%S.%fZ",
            ).date()

            type = tile_json["geometry"]["type"]

            if type == "MultiPolygon":
                tile = SatelliteImageTile(
                    Product=product,
                    tile_id=tile_json["id"],
                    date=tile_date,
                    boundary=Polygon(tile_json["geometry"]["coordinates"][0][0]),
                    to_be_downloaded=settings.TO_BE_DOWNLOADED,
                    eodag_data=tile_json,
                    New_Collection_ID=new_collection_id,
                )
                tile.save()
            elif type == "Polygon":
                tile = SatelliteImageTile(
                    Product=product,
                    tile_id=tile_json["id"],
                    date=tile_date,
                    boundary=Polygon(tile_json["geometry"]["coordinates"][0]),
                    to_be_downloaded=settings.TO_BE_DOWNLOADED,
                    eodag_data=tile_json,
                    New_Collection_ID=new_collection_id,
                )
                tile.save()
            else:
                raise ValueError("Unexpected geometry type and type is:", type)

            log(
                f"Mna New Collection id is: {new_collection_id}.",
                sat_image_tile=tile,
                level=logging.DEBUG,
                MethodName=self.update_to_database.__qualname__,StatusCode=500
            )
            log(
                f"Mna New Collection id from satellite tile table is: {tile.New_Collection_ID}.",
                sat_image_tile=tile,
                level=logging.DEBUG,
                MethodName=self.update_to_database.__qualname__,StatusCode=500
            )

            log(
                "Sentinel image tile created Successfully.",
                sat_image_tile=tile,
                level=logging.DEBUG,
                MethodName=self.update_to_database.__qualname__,StatusCode=200
            )
            # Add the SatelliteImageTile object to the list of tiles
            tiles.append(tile)
        return tiles

    def download(self, sat_image_tile: SatelliteImageTile, source_data: SourceData):
        log(
            "Downloading Sentinel image tile.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.INFO,
            MethodName=self.download.__qualname__,StatusCode=100
        ),

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
            log(
                "Creating temporary file for JSON data.",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
                MethodName=self.download.__qualname__,StatusCode=100
            ),

            # Write JSON data to the temporary file
            json.dump(
                {"type": "FeatureCollection", "features": [sat_image_tile.eodag_data]},
                temp_file,
            )

            # Remember the filename
            temp_filename = temp_file.name

            log(
                f"Temporary file created: {temp_filename}",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
                MethodName=self.download.__qualname__,StatusCode=100
            ),

        # Initialize EODataAccessGateway instance
        dag = EODataAccessGateway()

        # Deserialize data from the temporary file using EODataAccessGateway
        results = dag.deserialize(temp_filename)
        log(
            "Deserialize data from temporary file.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.download.__qualname__,StatusCode=100
        ),

        # After the process, delete the temporary file
        os.remove(temp_filename)

        log(
            f"Temporary file deleted: {temp_filename}",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.download.__qualname__,StatusCode=100
        ),

        # Increment download attempts and save the download start time for the satellite image tile
        sat_image_tile.dl_attempts += 1
        sat_image_tile.dl_start_time = timezone.now()
        log(
            "Incrementing download attempts for satellite image tile.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.download.__qualname__,StatusCode=100
        ),
        log(
            "Saving download start time for satellite image tile.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.download.__qualname__,StatusCode=100
        ),
        sat_image_tile.save()

        # Download the satellite image data using EODataAccessGateway
        dag.download(results[0], extract=False)

        # Mark the satellite image tile as downloaded and save the download end time
        sat_image_tile.is_downloaded = True
        log(
            "Marking satellite image tile as downloaded.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.download.__qualname__,StatusCode=100
        ),
        sat_image_tile.dl_end_time = timezone.now()
        log(
            "Saving download end time for satellite image tile.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.download.__qualname__,StatusCode=100
        ),
        sat_image_tile.save()

        log(
            "Download completed successfully.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.download.__qualname__,StatusCode=200
        ),

        return True

    def fetch_metadata(
        self, sat_image_tile: SatelliteImageTile, source_data: SourceData
    ):
        """Fetch metadata for the sentinel tile
        Make sure to have the same format as BusinessMeta to avoid confusion."""

        log(
            "Fetching metadata for satellite image tile.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.fetch_metadata.__qualname__,StatusCode=100
        ),

        log(
            "Checking if satellite image tile has downloaded from Sentinel 2",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.fetch_metadata.__qualname__,StatusCode=100
        )
        if sat_image_tile.Product == PRODUCT_SENTINEL2:
            # Initialize an empty dictionary to store metadata
            metadata = {}

            # Extract properties from the EODAG data
            properties = sat_image_tile.eodag_data.get("properties", {})

            geometry_type = sat_image_tile.eodag_data["geometry"]["type"]
            if geometry_type == "MultiPolygon":
                # Populate metadata with relevant properties
                metadata["Extent"] = Polygon(
                    sat_image_tile.eodag_data["geometry"]["coordinates"][0][0],
                    srid=4326,
                )
            elif geometry_type == "Polygon":
                metadata["Extent"] = Polygon(
                    sat_image_tile.eodag_data["geometry"]["coordinates"][0], srid=4326
                )
            else:
                log(
                    f"Invalid geometry type: {geometry_type}",
                    sat_image_tile=sat_image_tile,
                    source_data=source_data,
                    level=logging.DEBUG,
                    MethodName=self.fetch_metadata.__qualname__,StatusCode=100
                )

            metadata["Keywords"] = properties.get("keywords", None)
            metadata["CloudCover"] = properties.get("cloudCover", None)
            metadata["OrganizationName"] = properties.get("organisationName", None)
            metadata["ProcessingLevel"] = properties.get("processingLevel", None)
            metadata["Abstract"] = properties.get("abstract", None)
            metadata["SensorMode"] = properties.get("sensorMode", None)
            metadata["SensorType"] = properties.get("sensorType", None)
            metadata["ProductType"] = properties.get("productType", None)
            metadata["PlatformIdentifier"] = properties.get(
                "platformSerialIdentifier", None
            )
            metadata["Identifier"] = properties.get("parentIdentifier", None)
            metadata["LicenseBasedConstraints"] = properties.get("license", None)
            metadata["PlatformName"] = properties.get("platform", None)
            metadata["Title"] = properties.get("title", None)
            metadata["Resolution"] = properties.get("resolution", None)

            log(
                f"Successfully fetched metadata for Sentinel tile image and metadata is: {metadata}",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
            )
            # Return the fetched metadata
            return metadata

        else:
            log(
                f"No fetching logic found for fetching metadata {sat_image_tile.Product}",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
                MethodName=self.fetch_metadata.__qualname__,StatusCode=100
            )
            raise Exception(
                f"No fetching logic found for fetching metadata {sat_image_tile.Product}",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
                MethodName=self.fetch_metadata.__qualname__,StatusCode=500
            )

    def fetch_target_images(
        self, sat_image_tile: SatelliteImageTile, source_data: SourceData
    ):
        # Fetching target images for sentinel image tile.

        log(
            "Fetching target images for sentinel image tile.",
            sat_image_tile=sat_image_tile,
            source_data=source_data,
            level=logging.DEBUG,
            MethodName=self.fetch_target_images.__qualname__,StatusCode=100
        ),

        folder_path = sat_image_tile.extracted_path

        if sat_image_tile.Product == PRODUCT_SENTINEL2:
            log(
                "Fetching images for Sentinel 2 tile.",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
                MethodName=self.fetch_target_images.__qualname__,StatusCode=100
            ),
            # granule_folder = os.path.join(folder_path, os.listdir(folder_path)[0], 'GRANULE')
            # image_folder = os.path.join(granule_folder, os.listdir(granule_folder)[0], 'IMG_DATA')

            target_images = []

            log(
                "Checking every image in sentinel tile image folder.",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
                MethodName=self.fetch_target_images.__qualname__,StatusCode=100
            ),
            for image in os.listdir(folder_path):
                item_path = os.path.join(folder_path, image)

                # Check if it's a file
                if os.path.isfile(item_path):
                    # Generate a unique identifier (UUID)
                    unique_identifier = str(uuid.uuid4())

                    # Get the band name from the original filename
                    band_name = os.path.splitext(image)[0].split("_")[-1]

                    # Extract the extension from the original filename
                    extension = os.path.splitext(image)[1]

                    # Construct the new filename
                    new_filename = f"{unique_identifier}_{band_name}{extension}"

                    # Create the new file path
                    new_item_path = os.path.join(folder_path, new_filename)

                    # Rename the file
                    try:
                        os.rename(item_path, new_item_path)

                    except Exception as e:
                        log(
                            f"Error in renaming '{item_path}': {e} and {traceback.format_exc()}",
                            sat_image_tile=sat_image_tile,
                            source_data=source_data,
                            level=logging.DEBUG,
                            MethodName=self.fetch_target_images.__qualname__,StatusCode=500
                        )

                    # Add the renamed file to target_images
                    image_data = {
                        "Path": new_item_path,
                        "BandName": band_name,
                    }
                    log(
                        f"File name is renamed and added to target_images list and file data is: {image_data}",
                        sat_image_tile=sat_image_tile,
                        source_data=source_data,
                        level=logging.DEBUG,
                        MethodName=self.fetch_target_images.__qualname__,StatusCode=100
                    )
                    target_images.append(image_data)

        elif sat_image_tile.Product == PRODUCT_SENTINEL1:
            # granule_folder = os.path.join(folder_path, os.listdir(folder_path)[0], 'GRANULE')
            # image_folder = os.path.join(granule_folder, os.listdir(granule_folder)[0], 'IMG_DATA')

            # target_images = []
            # for image in os.listdir(image_folder):
            #     image_data = {
            #         'Path': os.path.join(image_folder, image),
            #         'BandName': os.path.splitext(image)[0].split("_")[-1]
            #     }
            #     target_images.append(image_data)
            pass
        else:
            log(
                f"No fetching logic found for product type {sat_image_tile.Product}.",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
                MethodName=self.fetch_target_images.__qualname__,StatusCode=400
            ),
            raise Exception(
                f"No fetching logic found for product type {sat_image_tile.Product}",
                sat_image_tile=sat_image_tile,
                source_data=source_data,
                level=logging.DEBUG,
                MethodName=self.fetch_target_images.__qualname__,StatusCode=404
            )

        return target_images

    def find_epsg(self, target_image_path: str) -> str:
        """This is for a hot fix where for the sentinel image,
        we can't get epsg code until extracted. To remove this and
        do this properly. move image extraction logic before metadata reading."""

        with rasterio.open(target_image_path) as dataset:
            return str(dataset.crs.to_epsg())
