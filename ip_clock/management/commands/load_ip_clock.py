from django.core.management import BaseCommand
from django.db.transaction import atomic
from ...luigi.tasks import CleanIPClock, CleanHandover, CleanSite, CleanNetwork, FilterHandover
from luigi import build
from csci_utils.luigi.dask.target import ParquetTarget
from ...models import DimGeo, FactHandover, FactNetworkElements, DimIpClock
import numpy as np


class Command(BaseCommand):
    help = "Load geo, ip clock and handover information"

    def handle(self, *args, **options):

        # load ip clock data into DB
        DimIpClock.objects.all().delete()
        build([CleanIPClock()], local_scheduler=True)
        dsk_ipclock = ParquetTarget('CleanIPClock/').read(columns=['ip_clock_name', 'ip_clock_type'])
        dsk_ipclock.ip_clock_type = dsk_ipclock.ip_clock_type.replace({np.nan: None})
        dsk_ipclock.ip_clock_name = dsk_ipclock.ip_clock_name.replace({np.nan: ''})
        with atomic():
            DimIpClock.objects.bulk_create(
                DimIpClock(**vals) for vals in dsk_ipclock.compute().to_dict('records')
            )

        # load geo data into DB
        DimGeo.objects.all().delete()
        build([CleanSite()], local_scheduler=True)
        dsk_site = ParquetTarget('CleanSite/').read()
        with atomic():
            DimGeo.objects.bulk_create(
                DimGeo(**vals) for vals in dsk_site.compute().to_dict('records')
            )

        # load geo data into DB
        FactNetworkElements.objects.all().delete()
        # get valid so_number from geo data
        valid_so_number = dsk_site.compute()['so_number'].tolist()
        valid_ip_clock_names = dsk_ipclock.compute()['ip_clock_name'].tolist()
        build([CleanNetwork()], local_scheduler=True)
        dsk_network = ParquetTarget('CleanNetwork/').read()
        with atomic():
            [FactNetworkElements.objects.get_or_create(
                ne_number=vals['ne_number'],
                so_number=DimGeo.objects.get(so_number=vals['so_number']),
                ip_clock_name=DimIpClock.objects.get(ip_clock_name=vals['ip_clock_name']),
            )[0] for vals in dsk_network.compute().to_dict('records')[:100000]
             if (vals['so_number'] in valid_so_number) & (vals['ip_clock_name'] in valid_ip_clock_names) ]
#or (vals['ip_clock_name'] == '')

        FactHandover.objects.all().delete()
        build([CleanHandover(subset=True)], local_scheduler=True)
        dsk_handover = ParquetTarget('CleanHandover/').read(columns=['source', 'target', 'attempts'])
        valid_ne_numbers = FactNetworkElements.objects.all().values_list('ne_number', flat=True)
        with atomic():
            [FactHandover.objects.get_or_create(
                source=FactNetworkElements.objects.get(ne_number=vals['source']),
                target=FactNetworkElements.objects.get(ne_number=vals['target']),
                attempts=vals['attempts'],
            )[0] for vals in dsk_handover.compute().to_dict('records')
             if (vals['target'] in valid_ne_numbers) and (vals['source'] in valid_ne_numbers)]



