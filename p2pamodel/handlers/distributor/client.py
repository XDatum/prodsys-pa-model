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

import numpy as np

from pyspark import SparkContext
from pyspark.sql import SQLContext

from ...providers.deftclient import DEFTClient as ExternalService

try:
    from ...providers.p2paclient import P2PAClient as InternalService
except ImportError:
    use_internal_service = False
else:
    use_internal_service = True

from ..constants import DataType
from ..settings import settings

COMPONENT_NAME = 'distributor'
CONFIG_FILE_NAME = 'cfg_distr'
THRESHOLD_PERCENTILE = 95
sc = SparkContext(appName=settings.SERVICE_NAME)


class Distributor(object):

    def __init__(self, **kwargs):
        """
        Initialization.

        @keyword data_dir: Data directory name.
        @keyword auth_user: User name for API.
        @keyword auth_key: User passphrase for API.
        @keyword verbose: Flag to get (show) logs.
        """
        self._source_path = '{0}/{1}'.format(
            settings.STORAGE_PATH_FORMAT.format(
                dir_name=kwargs.get('data_dir')
                or settings.WORK_DIR_NAME_DEFAULT),
            DataType.Output)

        api_credentials = getattr(__import__(
            '{0}.{1}'.format(
                settings.CONFIG_PKG_PATH_FORMAT.format(
                    component=COMPONENT_NAME),
                CONFIG_FILE_NAME),
            fromlist=['api_credentials']), 'api_credentials')

        self._external_user = kwargs.get(
            'auth_user', api_credentials.external.user)

        self._external_service = ExternalService(
            auth_user=self._external_user,
            auth_key=kwargs.get(
                'auth_key', api_credentials.external.passphrase),
            base_url=api_credentials.external.url)

        if use_internal_service:
            self._internal_service = InternalService(
                token=api_credentials.internal.token,
                base_url=api_credentials.internal.url)

        self._verbose = kwargs.get('verbose', False)

    def set_ttc_thresholds(self):
        """
        Get data from parquet-file, generate thresholds and upload to database.
        """
        sql_context = SQLContext(sc)
        raw_data = sql_context.read.parquet(self._source_path)

        prepared_data = raw_data.\
            map(lambda x: (
                '{0}.{1}.{2}'.format(x[0], x[1], x[2]), (x[3] / 1e3))).\
            groupByKey().\
            mapValues(lambda x: int(
                np.percentile(list(x), THRESHOLD_PERCENTILE)))

        output = prepared_data.collectAsMap()
        if output:
            self._external_service.set_ttcr(owner=self._external_user,
                                            ttcr_dict=output)

    def set_ttc_predictions(self):
        """
        Get text-file (<taskId,submitTime,duration>) and set ttcj_timestamp.
        """
        output = {}

        for task_id, submit_time, duration in sc.\
                textFile(self._source_path).\
                map(lambda x: x.split(',')).\
                filter(lambda x: len(x) > 1).\
                collect():

            output[int(task_id)] = \
                int((float(submit_time) + float(duration)) / 1e3)

        if output:
            self._external_service.set_ttcj(owner=self._external_user,
                                            ttcj_dict=output)

            if use_internal_service:
                self._internal_service.set_td_predictions(data=output)
