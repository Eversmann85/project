from csci_utils.luigi.dask.target import ParquetTarget, CSVTarget
from csci_utils.luigi.task import Requires, Requirement, TargetOutput
from luigi import ExternalTask, Parameter, Task, format, BoolParameter
from ..models import FactNetworkElements, FactHandover
import numpy as np
from django.db.transaction import atomic


class IPClock(ExternalTask):
    """Class which reads CSV files from S3 AWS server

    luigi param: path str: folder on S3 where csv files are stored

    luigi output: dask data frame
    """
    path = Parameter()

    def output(self):
        return ParquetTarget(self.path)


class CleanIPClock(Task):
    """Class which performs data cleaning

    luigi input: dask data frame

    luigi output: parquet files where dask data frame is written
    """

    requires = Requires()
    ip_clock = Requirement(IPClock(path='s3://project-eversmann/data/ip_clock/'))
    output = TargetOutput(target_class=ParquetTarget)

    def run(self):
        # read dask data frame object
        dsk = self.input()['ip_clock'].read()
        dsk.ip_clock_name = dsk.ip_clock_name.fillna('')
        dsk = dsk.drop_duplicates(subset=['ip_clock_name'])
        self.output().write(dsk, compression='gzip')


class GetParquet(ExternalTask):
    """Class which reads CSV files from S3 AWS server

    luigi param: path str: folder on S3 where csv files are stored

    luigi output: dask data frame
    """
    path = Parameter()

    def output(self):
        return ParquetTarget(self.path)

class CleanNetwork(Task):
    """Class which performs data cleaning

    luigi input: dask data frame

    luigi output: parquet files where dask data frame is written
    """

    requires = Requires()
    network_elements = Requirement(GetParquet(path='s3://project-eversmann/data/network_elements/'))
    ip_clock = Requirement(IPClock(path='s3://project-eversmann/data/ip_clock/'))
    output = TargetOutput(target_class=ParquetTarget)

    def run(self):
        # read dask data frame object
        dsk_network = self.input()['network_elements'].read()
        dsk_ip_clock = self.input()['ip_clock'].read(columns=['ne_number', 'ip_clock_name'])
        dsk_ip_clock.ip_clock_name = dsk_ip_clock.ip_clock_name.fillna('')
        dsk_network = dsk_network.dropna()
        dsk_network = dsk_network[dsk_network['so_number'].apply(lambda x: str(x).isdigit())]
        dsk_network = dsk_network[dsk_network['ne_number'].apply(lambda x: str(x).isdigit())]
        dsk_network.ne_number = dsk_network.ne_number.astype('int')
        dsk_network = dsk_network.merge(dsk_ip_clock, how='left', left_on='ne_number', right_on='ne_number')
        self.output().write(dsk_network[['ne_number', 'so_number', 'ip_clock_name']], compression='gzip')


class CleanSite(Task):
    """Class which performs data cleaning

    luigi input: dask data frame

    luigi output: parquet files where dask data frame is written
    """

    requires = Requires()
    site = Requirement(GetParquet(path='s3://project-eversmann/data/sites/'))
    output = TargetOutput(target_class=ParquetTarget)

    def run(self):
        # read dask data frame object
        dsk_site = self.input()['site'].read()
        dsk_site = dsk_site.dropna()
        dsk_site = dsk_site[dsk_site['so_number'].apply(lambda x: str(x).isdigit())]
        self.output().write(dsk_site, compression='gzip')


class CleanHandover(Task):
    """Class which performs data cleaning

    luigi input: dask data frame

    luigi output: parquet files where dask data frame is written
    """

    subset = BoolParameter(default=True)
    requires = Requires()
    handover = Requirement(GetParquet(path='s3://project-eversmann/data/handover/'))
    output = TargetOutput(target_class=ParquetTarget)

    def run(self):
        # read dask data frame object
        dsk_handover = self.input()['handover'].read()
        if self.subset:
            dsk_handover = dsk_handover.get_partition(0)
        self.output().write(dsk_handover, compression='gzip')


class FilterHandover(Task):
    """Class which filters handover by distance
    """

    def run(self):
        with atomic():
            geo_source = np.array(list(FactHandover.objects.values_list(
                    'source__so_number__latitude_wgs84',
                    'source__so_number__longitude_wgs84',
                )))

            geo_target = np.array(list(FactHandover.objects.values_list(
                    'target__so_number__latitude_wgs84',
                    'target__so_number__longitude_wgs84',
                )))

        a_min_b = (geo_source - geo_target)*np.array([1, 0.66])
        print(geo_source[99])
        print(geo_target[99])
        print(a_min_b[99])
        # https://jonisalonen.com/2014/computing-distance-between-coordinates-can-be-simple-and-fast/
        distance = 110.25*np.sqrt(np.einsum("ij,ij->i", a_min_b, a_min_b))
        print(distance[99])
        print(np.where(distance > 50)[0])


