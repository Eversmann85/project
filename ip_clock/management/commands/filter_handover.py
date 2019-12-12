from django.core.management import BaseCommand
from ...luigi.tasks import FilterHandover
from luigi import build


class Command(BaseCommand):
    help = "Filter Handover Data"

    def handle(self, *args, **options):
        build([FilterHandover()], local_scheduler=True)