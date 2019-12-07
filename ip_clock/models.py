from django.db.models import *

# Create your models here.


class FactIpClock(Model):
    ne_number = IntegerField(default=0)  # ne number
    ip_clock_name = CharField(max_length=64)  # ip clock name
    ip_clock_type = IntegerField(default=0, null=True, blank=True)  # ip clock type
