from django.db.models import *

# Create your models here.


class DimIpClock(Model):
    ip_clock_name = CharField(max_length=64)  # ip clock name
    ip_clock_type = IntegerField(default=0, null=True, blank=True)  # ip clock type

    class Meta:
        unique_together = ['ip_clock_name']


class DimGeo(Model):
    so_number = IntegerField(default=0)  # ne number
    latitude_wgs84 = FloatField(default=0)  # ip clock name
    longitude_wgs84 = FloatField(default=0)  # ip clock typ
    kgs12 = IntegerField(default=0)  # ip clock typ

    class Meta:
        unique_together = ['so_number']


class FactNetworkElements(Model):
    ne_number = IntegerField(default=0)  # ne number
    so_number = ForeignKey(DimGeo, on_delete=CASCADE)  # ForeignKey
    ip_clock_name = ForeignKey(DimIpClock, on_delete=CASCADE)  # ForeignKey

    class Meta:
        unique_together = ['ne_number']


class FactHandover(Model):
    source = ForeignKey(FactNetworkElements, on_delete=CASCADE, related_name='source')  # ne number source
    target = ForeignKey(FactNetworkElements, on_delete=CASCADE, related_name='target')  # ne number target
    attempts = IntegerField(default=0)  # number of handover attempts


