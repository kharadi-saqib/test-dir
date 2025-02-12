from django.core.managemenet.base import BaseCommand, CommandError

from SatProductCurator.workers import SatProductSearchWorker
from SatProductCurartor.models.constants import PRODUCT_SENTINEL2
from SatProductCurartor.services import SentinelTileService
from django.contrib.gis.geos import Polygon

import datetime

class Command(BaseCommand):
    help = "Closes the specified poll for voting"
    def handle(self, *args, **options):
        service = SentinelTileService()
        service.search_by_polygon(
            PRODUCT_SENTINEL2,
            datetime.date(2023, 1, 1),
            datetime.date(2023,1,10),
            polygon=Polygon([(54.3515 24.2482),(54.3515 24.5338),(54.6371 24.5338),(54.6371 24.2482),(54.3515 24.2482)])
        )
