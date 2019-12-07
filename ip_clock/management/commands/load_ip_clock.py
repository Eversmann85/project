from django.core.management import BaseCommand
from django.test import override_settings
from django.db.transaction import atomic
from ...luigi.tasks import CleanIPClock
from luigi import build
from csci_utils.luigi.dask.target import ParquetTarget
from ...models import FactIpClock
import numpy as np


class Command(BaseCommand):
    help = "Load review facts"

    def handle(self, *args, **options):
        FactIpClock.objects.all().delete()
        build([CleanIPClock()], local_scheduler=True)
        # use parquet logic from pset-5
        dsk = ParquetTarget('CleanIPClock/').read(columns=['ne_number', 'ip_clock_name', 'ip_clock_type'])
        dsk.ip_clock_type = dsk.ip_clock_type.replace({np.nan: None})
        dsk.ip_clock_name = dsk.ip_clock_name.replace({np.nan: ''})
        with atomic():
            FactIpClock.objects.bulk_create(
                FactIpClock(**vals) for vals in
                            dsk[['ne_number', 'ip_clock_name', 'ip_clock_type']].compute().to_dict('records')
            )
