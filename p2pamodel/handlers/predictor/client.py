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
from pyspark.mllib.regression import LabeledPoint

from ..settings import (DataType,
                        STORAGE_PATH_FORMAT,
                        WORK_DIR_NAME_DEFAULT)

from .config import (SERVICE_NAME,
                     TRAINING_OPTIONS,
                     LABELED_POINTS,
                     SELECT_PARAMS)

_pa_method = GradientBoostedTrees
_pa_model = GradientBoostedTreesModel

sc = SparkContext(appName=SERVICE_NAME)


class Predictor(object):

    def __init__(self, **kwargs):
        """
        Initialization.

        @keyword work_dir: Working directory name.
        @keyword test_data_dir: Directory with test data.
        @keyword input_data_dir: Directory with input/new data.
        @keyword verbose: Flag to get (show) logs.
        """
        self._data = {}
        self._paths = {}
        self._work_dir_path = STORAGE_PATH_FORMAT.format(
            work_dir=kwargs.get('work_dir') or WORK_DIR_NAME_DEFAULT)

        self._test_data_path = kwargs.get('test_data_dir')
        self._input_data_path = kwargs.get('input_data_dir')

        self._verbose = kwargs.get('verbose', False)

    @property
    def _model(self):
        return self._data.get(DataType.Model)

    @_model.setter
    def _model(self, value):
        if value is not None:
            self._data[DataType.Model] = value

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
        return self._data.get(DataType.Input)

    @_input_data.setter
    def _input_data(self, value):
        if value is not None:
            self._data[DataType.Input] = value

    def _get_path(self, data_type):
        output = None
        if data_type in [DataType.Model, DataType.Eval, DataType.Training]:
            output = '{0}/{1}'.format(self._work_dir_path, data_type)
        elif data_type in [DataType.Test, DataType.Input]:
            output = self._paths.get(data_type)
        return output

    def _set_path(self, data_type, dir_name):
        if data_type in [DataType.Test, DataType.Input] and dir_name:
            self._paths[data_type] = STORAGE_PATH_FORMAT.\
                format(work_dir=dir_name)

    @property
    def _model_path(self):
        return self._get_path(data_type=DataType.Model)

    @property
    def _model_eval_path(self):
        return self._get_path(data_type=DataType.Eval)

    @property
    def _training_data_path(self):
        return self._get_path(data_type=DataType.Training)

    @property
    def _test_data_path(self):
        return self._get_path(data_type=DataType.Test)

    @_test_data_path.setter
    def _test_data_path(self, value):
        self._set_path(data_type=DataType.Test, dir_name=value)

    @property
    def _input_data_path(self):
        return self._get_path(data_type=DataType.Input)

    @_input_data_path.setter
    def _input_data_path(self, value):
        self._set_path(data_type=DataType.Input, dir_name=value)

    def prepare_data(self, data_types):
        """
        Prepare data sets (training, test, input).

        @param data_types: Flags to define which data should be prepared.
        @type data_types: list
        """
        filter_query = 'TASKID=PARENT_TID AND NUCLEUS IS NOT NULL'

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

            if (data_type not in DataType.attrs.values() or
                    not self._get_path(data_type)):
                continue

            raw_data = sql_context.read.parquet(self._get_path(data_type))
            filtered_data = raw_data.filter(filter_query).select(*SELECT_PARAMS)
            parsed_data = filtered_data.map(create_labeled_point)

            if data_type == DataType.Training:
                if DataType.Test in data_types and not self._test_data_path:
                    self._training_data, self._test_data = \
                        parsed_data.randomSplit([0.7, 0.3])
                elif DataType.Test not in data_types:
                    self._training_data = parsed_data

            elif data_type == DataType.Test:
                self._test_data = parsed_data

            elif data_type == DataType.Input:
                self._input_data = parsed_data

# if `data_types = [DataType.Test]` and `test_data_path` is not set
# then there is no `test_data`; no `test_data_path` should work only
# with `[DataType.Training, DataType.Test]`

    def load_model(self):
        """
        Load the model (by using model_path based on work_dir).
        """
        self._model = _pa_model.load(sc, self._model_path)

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
                map(lambda x: (x[0], len(LABELED_POINTS[x])), LABELED_POINTS))

        try:
            self._model = _pa_method.trainRegressor(
                data=self._training_data,
                categoricalFeaturesInfo=get_categorical_features_info(),
                **TRAINING_OPTIONS)
        except ValueError, e:
            raise Exception('[ERROR] Exception in model training: {0}'.format(e))
        else:
            self._model.save(sc, self._model_path)

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
            file_path = ('{0}/labels_with_predictions'.
                         format(self._model_eval_path))
            lines = labels_with_predictions.map(to_csv_line)
            lines.saveAsTextFile(file_path)
        except:
            pass

        abs_errors = labels_with_predictions.map(lambda (v, p): v - p)
        rel_errors = labels_with_predictions.map(lambda (v, p): (v - p) / v)
        try:
            file_path = ('{0}/abs_errors'.format(self._model_eval_path))
            lines = abs_errors.map(to_csv_line)
            lines.saveAsTextFile(file_path)

            file_path = ('{0}/rel_errors'.format(self._model_eval_path))
            lines = rel_errors.map(to_csv_line)
            lines.saveAsTextFile(file_path)
        except:
            pass

# - general purpose methods -

    def run_trainer(self, with_eval=False, test_data_dir=None):
        """
        Run model training/testing process.

        @param with_eval: Flag to proceed model evaluation.
        @type with_eval: bool
        @param test_data_dir: Directory with test data.
        @type test_data_dir: str/None
        """
        self._test_data_path = test_data_dir

        data_types = [DataType.Training]
        if with_eval:
            data_types.append(DataType.Test)
        self.prepare_data(data_types=data_types)

        self.create_model()

        if with_eval:
            self.evaluate_model()

    def run_evaluator(self, reload_model=False, test_data_dir=None):
        """
        Run model evaluation process.

        @param reload_model: Flag to force to re-load model.
        @type reload_model: bool
        @param test_data_dir: Directory with test data.
        @type test_data_dir: str/None
        """
        self._test_data_path = test_data_dir

        self.prepare_data(data_types=[DataType.Test])

        if self._model is None or reload_model:
            self.load_model()

        self.evaluate_model()

    def run_predictor(self, reload_model=False, input_data_dir=None):
        """
        Run prediction process.

        @param reload_model: Flag to force to re-load model.
        @type reload_model: bool
        @param input_data_dir: Directory with input data.
        @type input_data_dir: str/None
        @return: Predictions.
        @rtype: -
        """
        self._input_data_path = input_data_dir

        self.prepare_data(data_types=[DataType.Input])

        if self._model is None or reload_model:
            self.load_model()

        return self.get_predictions(is_eval=False)


# TODO: use database to store LABELED_POINTS data
def prepare_labeled_points():
    """
    Prepare labeled points.
    """
    raise NotImplementedError
