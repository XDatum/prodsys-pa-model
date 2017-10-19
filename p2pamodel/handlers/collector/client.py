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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017
#

import os

from ...utils import pyCMD
from ..settings import (DataType,
                        HDFS_DATA_DIR,
                        HDFS_PRIVATE_DIR,
                        WORK_DIR_NAME_DEFAULT)

from .config import (DAYS_DEFAULT,
                     DAYS_OFFSET_DEFAULT)

CONFIG_PKG_PATH = 'p2pamodel.handlers.collector.config'


def create_hdfs_private_file(service_name, message):
    """
    Create private file at HDFS storage.

    @param service_name: Service name.
    @type service_name: str
    @param message: Private message.
    @type message: str
    @return: Full file path.
    @rtype: str
    """
    file_name = '{0}.sqoop'.format(service_name)
    file_path = '{0}/{1}'.format(HDFS_PRIVATE_DIR, file_name)

    with open(file_name, 'w') as f:
        f.write(message)

    pyCMD('hdfs', ['dfs', '-put', file_name, file_path]).execute()
    pyCMD('hdfs', ['dfs', '-chown', '400', file_path]).execute()

    os.remove(file_name)
    return file_path


def remove_hdfs_file(path):
    """
    Remove file from HDFS storage.

    @param path: Full file path.
    @type path: str
    """
    pyCMD('hdfs', ['dfs', '-rm', '-skipTrash', path]).execute()


def remove_hdfs_dir(path):
    """
    Remove directory from HDFS storage.

    @param path: Full dir path.
    @type path: str
    """
    pyCMD('hdfs', ['dfs', '-rm', '-r', '-f', '-skipTrash', path]).execute()


class Collector(object):

    def __init__(self, **kwargs):
        """
        Initialization.

        @keyword work_dir: Working directory name.
        @keyword config: Configuration module name (in .config sub-pkg).
        @keyword verbose: Flag to get (show) logs.
        """
        if not kwargs.get('config'):
            raise Exception('Configuration module name is not set.')

        self._work_dir = '{0}/{1}'.format(
            HDFS_DATA_DIR, kwargs.get('work_dir') or WORK_DIR_NAME_DEFAULT)

        self._config = getattr(__import__(
            '{0}.{1}'.format(CONFIG_PKG_PATH, kwargs['config']),
            fromlist=['config']), 'config')

        self._verbose = kwargs.get('verbose', False)

    def _get_dir(self, source):
        return '{0}/{1}'.format(self._work_dir, source)

    @staticmethod
    def _form_sql_time_period(column_name, days=None, days_offset=None):
        """
        Form SQL query part that is responsible for time period.

        @param column_name: Datetime column name.
        @type column_name: str
        @param days: Number of days (period time).
        @type days: int/None
        @param days_offset: Number of days for offset.
        @type days_offset: int/None
        @return: SQL format for time period statement.
        @rtype: str
        """
        days = days or DAYS_DEFAULT
        days_offset = days_offset or DAYS_OFFSET_DEFAULT

        return ' AND '.join([
            "{0} >= CURRENT_DATE - {1}".format(
                column_name,
                days + days_offset),
            "{0} < CURRENT_DATE{1}".format(
                column_name,
                '' if not days_offset else ' - {0}'.format(days_offset))])

    def _get_sql_query(self, query_specs, days, days_offset):
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
                [self._form_sql_time_period(
                    column_name=query_specs.time_range_column,
                    days=days,
                    days_offset=days_offset)] +
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

            password_file = create_hdfs_private_file(
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

            remove_hdfs_file(password_file)

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
            remove_hdfs_dir(self._get_dir(source='{0}'.format(source)))

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
            remove_hdfs_dir(self._get_dir(output_type))

        self.convert_data(output_dir_name=output_type)

        self.cleanup()
