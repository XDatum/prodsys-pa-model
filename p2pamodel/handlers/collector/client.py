#
# Copyright European Organization for Nuclear Research (CERN),
#           National Research Centre "Kurchatov Institute" (NRC KI)
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017-2018
#

from ...tools import hdfs
from ...utils import pyCMD

from ..constants import DataType
from ..settings import settings

COMPONENT_NAME = 'collector'


class Collector(object):

    def __init__(self, **kwargs):
        """
        Initialization.

        @keyword work_dir: Working directory name.
        @keyword config: Configuration module name (in config-sub-pkg).
        @keyword verbose: Flag to get (show) logs.
        """
        if not kwargs.get('config'):
            raise Exception('Configuration module name is not set.')

        self._work_dir = '{0}/{1}'.format(
            settings.HDFS_DATA_DIR,
            kwargs.get('work_dir') or settings.WORK_DIR_NAME_DEFAULT)

        self._config = getattr(__import__(
            '{0}.{1}'.format(
                settings.CONFIG_PKG_PATH_FORMAT.format(
                    component=COMPONENT_NAME),
                kwargs['config']),
            fromlist=['config']), 'config')

        self._verbose = kwargs.get('verbose', False)

    def _get_dir(self, source):
        return '{0}/{1}'.format(self._work_dir, source)

    @staticmethod
    def _get_sql_query(query_specs, days, days_offset):
        """
        Form SQL query for Sqoop command.

        @param query_specs: Query specifications (config).
        @type query_specs: utils.ConfigBase
        @param days: Number of days for the requested period.
        @type days: int
        @param days_offset: Number of days to offset.
        @type days_offset: int
        @return: SQL query.
        @rtype: str
        """
        return 'SELECT {0} FROM {1} WHERE {2}'.format(
            ', '.join(query_specs.select_columns),
            query_specs.table,
            ' AND '.join(
                ['{column} >= {min_date} AND {column} < {max_date}'.format(
                    column=query_specs.time_range_column,
                    min_date='CURRENT_DATE - {0}'.format(days + days_offset),
                    max_date='CURRENT_DATE{0}'.format(
                        '' if not days_offset
                        else ' - {0}'.format(days_offset)))] +
                query_specs.conditions +
                ['$CONDITIONS'])
        )

    def import_data(self, days, days_offset):
        """
        Get data from DEfT/JEDI by Sqoop.

        @param days: Number of days for the requested period.
        @type days: int
        @param days_offset: Number of days to offset.
        @type days_offset: int
        """
        sqoop_client = pyCMD(command='sqoop')

        for source in self._config.sqoop:

            src_name = '{0}'.format(source)

            password_file = hdfs.create_private_file(
                dir_name=settings.HDFS_PRIVATE_DIR,
                service_name=src_name,
                message=source.db.passphrase)

            sqoop_client.set_options('import', **{
                '--connect': source.db.jdbc,
                '--username': source.db.user,
                '--password-file': password_file,
                '--target-dir': self._get_dir(source=src_name),
                '--query': self._get_sql_query(query_specs=source.query,
                                               days=days,
                                               days_offset=days_offset)
            })
            sqoop_client.add_options(*source.options)
            result = sqoop_client.execute()

            hdfs.remove_file(password_file)

            if self._verbose:
                print('Running command: {0}'.format(
                    ' '.join(sqoop_client.command)))
                print 'Result: {0}, {1}\nDebug: {2}'.format(*result)

    def convert_data(self, output_dir_name):
        """
        Process imported data by Pig.

        @param output_dir_name: Name of the directory for output data.
        @type output_dir_name: str
        """
        pig_client = pyCMD(command='pig')
        pig_client.set_options(*self._config.pig.options)

        for source in self._config.sqoop:
            src_name = '{0}'.format(source)
            pig_client.add_option('-p', '{0}_tasks={1}'.format(
                                  src_name, self._get_dir(src_name)))
        pig_client.add_option('-p', 'out={0}'.format(
                              self._get_dir(output_dir_name)))
        result = pig_client.execute()

        if self._verbose:
            print('Running command: {0}'.format(
                ' '.join(pig_client.command)))
            print 'Result: {0}, {1}\nDebug: {2}'.format(*result)

    def cleanup(self):
        """
        Clean-up temporary files/dirs.
        """
        for source in self._config.sqoop:
            hdfs.remove_dir(self._get_dir(source='{0}'.format(source)))

    def execute(self, **kwargs):
        """
        Execute data import.

        @keyword days: Number of days for the requested period.
        @keyword days_offset: Number of days to offset.
        @keyword output_type: Data output type.
        @keyword force: Force to (re)create the output directory.
        """
        self.import_data(days=kwargs.get('days'),
                         days_offset=kwargs.get('days_offset'))

        output_type = kwargs.get('output_type')
        if output_type not in DataType.attrs.values():
            output_type = DataType.Training

        if kwargs.get('force'):
            hdfs.remove_dir(self._get_dir(output_type))

        self.convert_data(output_dir_name=output_type)

        self.cleanup()
