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
# - Maksim Gubin, <maksim.gubin@cern.ch>, 2016
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017
#

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.regression import LabeledPoint

from ...tools import hdfs

from ..settings import (DataType,
                        DirType,
                        STORAGE_PATH_FORMAT,
                        WORK_DIR_NAME_DEFAULT)

from .config import (SERVICE_NAME,
                     TRAINING_OPTIONS,
                     LABELED_POINTS,
                     SELECT_PARAMS)

TASK_ID_COLUMN = 'TASKID'
IS_RF_METHOD = True

if not IS_RF_METHOD:
    _pa_method = GradientBoostedTrees
    _pa_model = GradientBoostedTreesModel
    _pa_key = 'gbt'
else:
    _pa_method = RandomForest
    _pa_model = RandomForestModel
    _pa_key = 'rf'

_training_options = TRAINING_OPTIONS[_pa_key]

sc = SparkContext(appName=SERVICE_NAME)


class Predictor(object):

    def __init__(self, **kwargs):
        """
        Initialization.

        @keyword work_dir: Working directory name.
        @keyword data_dir: Directory with any data.
        @keyword verbose: Flag to get (show) logs.
        """
        self._model = None

        self._data = {}
        self._paths = {}

        work_dir = kwargs.get('work_dir') or WORK_DIR_NAME_DEFAULT
        self._set_dir_path(dir_type=DirType.Work, dir_name=work_dir)
        self._set_dir_path(dir_type=DirType.Data, dir_name=kwargs.get('data_dir'))

        self._verbose = kwargs.get('verbose', False)

    @property
    def _work_dir_path(self):
        return self._paths.get(DirType.Work)

    @property
    def _data_dir_path(self):
        return self._paths.get(DirType.Data)

    def _set_dir_path(self, dir_type, dir_name):
        if dir_type and dir_name:
            self._paths[dir_type] = STORAGE_PATH_FORMAT.format(dir_name=dir_name)

    def _get_path(self, data_type):
        base_dir_path = None

        if data_type not in [DataType.Model, DataType.Eval]:
            base_dir_path = self._data_dir_path

        if base_dir_path is None:
            base_dir_path = self._work_dir_path

        return '{0}/{1}'.format(base_dir_path, data_type)

    @property
    def _training_data(self):
        return self._data.get(DataType.Training)

    @_training_data.setter
    def _training_data(self, value):
        if value is not None:
            self._data[DataType.Training] = value

    @property
    def _test_data(self):
        return self._data.get(DataType.Test)

    @_test_data.setter
    def _test_data(self, value):
        if value is not None:
            self._data[DataType.Test] = value

    @property
    def _input_data(self):
        if DataType.Input in self._data:
            return self._data[DataType.Input].map(lambda x: x[1])

    @_input_data.setter
    def _input_data(self, value):
        if value is not None:
            self._data[DataType.Input] = value

    @property
    def _input_data_ids(self):
        if DataType.Input in self._data:
            return self._data[DataType.Input].map(lambda x: x[0])

    def prepare_data(self, data_types):
        """
        Prepare data sets (training, test, input).

        @param data_types: Flags to define which data should be prepared.
        @type data_types: list
        """
        # TODO: should be reworked
        #filter_query = 'TASKID=PARENT_TID AND NUCLEUS IS NOT NULL'

        def create_labeled_point(record):
            """
            Transforms categorical features in a record to numbers (indices).

            @param record: Data record.
            @type record: list/tuple
            @return: Object with corresponding labeled points.
            @rtype: LabeledPoint
            """
            record_data = list(record[1:])

            for label in LABELED_POINTS:
                idx = label[0]

                try:
                    record_data[idx] = LABELED_POINTS[label].index(
                        record_data[idx])
                except:
                    record_data[idx] = len(LABELED_POINTS[label])

            return LabeledPoint(record[0], [float(x) for x in record_data])

        sql_context = SQLContext(sc)

        for data_type in data_types:

            if data_type not in DataType.attrs.values():
                continue

            raw_data = sql_context.read.parquet(self._get_path(data_type))

            select_params = SELECT_PARAMS[:]
            if data_type == DataType.Input:
                select_params.insert(0, TASK_ID_COLUMN)

            #filtered_data = raw_data.filter(filter_query).select(*select_params)
            filtered_data = raw_data.select(select_params)

            if data_type == DataType.Input:
                self._input_data = filtered_data.map(
                    lambda x: (x[0], create_labeled_point(x[1:])))
            else:
                parsed_data = filtered_data.map(create_labeled_point)

                if data_type == DataType.Training:
                    self._training_data = parsed_data

                elif data_type == DataType.Test:
                    self._test_data = parsed_data

    def load_model(self):
        """
        Load the model (by using model path based on work_dir).
        """
        self._model = _pa_model.load(sc, self._get_path(DataType.Model))

    def create_model(self):
        """
        Create the model (by training data set).
        """
        def get_categorical_features_info():
            """
            Get categorical features info.

            @return: Feature id with the total number of elements.
            @rtype: dict
            """
            return dict(
                map(lambda x: (x[0], len(LABELED_POINTS[x]) + 1), LABELED_POINTS))

        try:
            self._model = _pa_method.trainRegressor(
                data=self._training_data,
                categoricalFeaturesInfo=get_categorical_features_info(),
                **_training_options)
        except ValueError, e:
            raise Exception('[ERROR] Exception in model training: {0}'.format(e))
        else:
            self._model.save(sc, self._get_path(DataType.Model))

    def get_predictions(self, is_eval=False):
        """
        Create predictions for test or input data sets.

        @param is_eval: Flag for model evaluation process.
        @type is_eval: bool
        @return: Predictions.
        @rtype: -
        """
        _data = self._test_data if is_eval else self._input_data
        return self._model.predict(_data.map(lambda x: x.features))

    def evaluate_model(self):
        """
        Model evaluation process.
        """
        def to_csv_line(record):
            if not isinstance(record, (list, tuple)):
                record = [record]
            return ','.join(str(r) for r in record)

        predictions = self.get_predictions(is_eval=True)

        labels_with_predictions = (self._test_data.
                                   map(lambda x: x.label).
                                   zip(predictions))
        try:
            labels_with_predictions.map(to_csv_line).saveAsTextFile(
                '{0}/labels_predictions'.format(self._get_path(DataType.Eval)))
        except:
            pass

        abs_errors = labels_with_predictions.map(lambda (v, p): v - p)
        rel_errors = labels_with_predictions.map(lambda (v, p): (v - p) / v)
        try:
            abs_errors.map(to_csv_line).saveAsTextFile(
                '{0}/abs_errors'.format(self._get_path(DataType.Eval)))
            rel_errors.map(to_csv_line).saveAsTextFile(
                '{0}/rel_errors'.format(self._get_path(DataType.Eval)))
        except:
            pass

# - general purpose methods -

    def print_defined_options(self):
        """
        Print defined options.
        """
        raise NotImplementedError

    def run_trainer(self, with_eval=False, data_dir=None, **kwargs):
        """
        Run model training/testing process.

        @param with_eval: Flag to proceed model evaluation.
        @type with_eval: bool
        @param data_dir: Directory with training/test data.
        @type data_dir: str/None

        @keyword force: Force to (re)create the model directory.
        """
        self._set_dir_path(dir_type=DirType.Data, dir_name=data_dir)

        data_types = [DataType.Training]
        if with_eval:
            data_types.append(DataType.Test)
        self.prepare_data(data_types=data_types)

        if kwargs.get('force'):
            hdfs.remove_dir(self._get_path(DataType.Model))

        self.create_model()

        if with_eval:
            self.evaluate_model()

    def run_evaluator(self, reload_model=False, data_dir=None, **kwargs):
        """
        Run model evaluation process.

        @param reload_model: Flag to force to re-load model.
        @type reload_model: bool
        @param data_dir: Directory with test data.
        @type data_dir: str/None

        @keyword force: Force to (re)create the eval directory.
        """
        self._set_dir_path(dir_type=DirType.Data, dir_name=data_dir)

        self.prepare_data(data_types=[DataType.Test])

        if self._model is None or reload_model:
            self.load_model()

        if kwargs.get('force'):
            hdfs.remove_dir(self._get_path(DataType.Eval))

        self.evaluate_model()

    def run_predictor(self, reload_model=False, data_dir=None, **kwargs):
        """
        Run prediction process.

        @param reload_model: Flag to force to re-load model.
        @type reload_model: bool
        @param data_dir: Directory with input data.
        @type data_dir: str/None

        @keyword force: Force to (re)create the eval directory.
        """
        self._set_dir_path(dir_type=DirType.Data, dir_name=data_dir)

        self.prepare_data(data_types=[DataType.Input])

        if self._model is None or reload_model:
            self.load_model()

        predictions = self.get_predictions(is_eval=False)

        ids_with_predictions = (self._input_data_ids.
                                map(lambda x: int(x)).
                                zip(predictions))

        def to_csv_line(record):
            if not isinstance(record, (list, tuple)):
                record = [record]
            return ','.join(str(r) for r in record)

        if kwargs.get('force'):
            hdfs.remove_dir(self._get_path(DataType.Output))

        ids_with_predictions.map(to_csv_line).saveAsTextFile(
            self._get_path(DataType.Output))


# TODO: use database to store LABELED_POINTS data
def prepare_labeled_points():
    """
    Prepare labeled points.
    """
    raise NotImplementedError
