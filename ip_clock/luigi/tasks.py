from csci_utils.luigi.dask.target import ParquetTarget, CSVTarget
from csci_utils.luigi.task import Requires, Requirement, TargetOutput
from luigi import ExternalTask, Parameter, Task, format, BoolParameter


class IPClock(ExternalTask):
    """Class which reads CSV files from S3 AWS server

    luigi param: path str: folder on S3 where csv files are stored

    luigi output: dask data frame
    """
    path = Parameter()

    def output(self):
        return CSVTarget(self.path)


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
        dsk = dsk.drop_duplicates()
        self.output().write(dsk, compression='gzip')

