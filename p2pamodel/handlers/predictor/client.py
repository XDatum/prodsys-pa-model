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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017-2018
#

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.regression import LabeledPoint

from ...tools import hdfs

from ..constants import DataType, DirType
from ..settings import settings

COMPONENT_NAME = 'predictor'
COL_SEPARATOR = ','

IS_RF_METHOD = True
if not IS_RF_METHOD:
    _pa_method = GradientBoostedTrees
    _pa_model = GradientBoostedTreesModel
    _pa_key = 'gbt'
else:
    _pa_method = RandomForest
    _pa_model = RandomForestModel
    _pa_key = 'rf'

sc = SparkContext(appName=settings.SERVICE_NAME)


class Predictor(object):

    def __init__(self, **kwargs):
        """
        Initialization.

        @keyword work_dir: Working directory name.
        @keyword data_dir: Directory with any data.
        @keyword config: Configuration module name (in config-sub-pkg).
        @keyword verbose: Flag to get (show) logs.
        """
        self._model = None
        self._paths = {}

        work_dir = kwargs.get('work_dir') or settings.WORK_DIR_NAME_DEFAULT
        self._set_dir_path(dir_type=DirType.Work,
                           dir_name=work_dir)
        self._set_dir_path(dir_type=DirType.Data,
                           dir_name=kwargs.get('data_dir'))

        self._config = getattr(__import__(
            '{0}.{1}'.format(
                settings.CONFIG_PKG_PATH_FORMAT.format(
                    component=COMPONENT_NAME),
                kwargs['config']),
            fromlist=['config']), 'config')

        self._verbose = kwargs.get('verbose', False)

    @property
    def _work_dir_path(self):
        return self._paths.get(DirType.Work)

    @property
    def _data_dir_path(self):
        return self._paths.get(DirType.Data)

    def _set_dir_path(self, dir_type, dir_name):
        if dir_type and dir_name:
            self._paths[dir_type] = settings.STORAGE_PATH_FORMAT.\
                format(dir_name=dir_name)

    def _get_path(self, data_type):
        base_dir_path = None

        if data_type not in [DataType.Model, DataType.Eval, DataType.Domain]:
            base_dir_path = self._data_dir_path

        if base_dir_path is None:
            base_dir_path = self._work_dir_path

        return '{0}/{1}'.format(base_dir_path, data_type)

    def _generate_labeled_point_values(self, data, num_skip=None, save=False):
        """
        Generate a dictionary of valid (possible) values for defined features.

        @param data: Input data.
        @type data: DataFrame
        @param num_skip: Number of elements to skip in data records.
        @type num_skip: int/None (default=1)
        @param save: Flag to save processed data (DataType.Domain).
        @type save: bool
        @return: Possible values for categorical features.
        @rtype: dict
        """
        output = {}

        num_skip = num_skip or 1
        for idx, (column_name, column_flag) in enumerate(
                self._config.feature_parameters):

            if column_flag:
                output[column_name] = sorted(
                    data.map(lambda x: x[idx + num_skip]).distinct().collect())

                if None in output[column_name]:
                    output[column_name].remove(None)

        if save:
            parsed_data = map(
                lambda (k, v): COL_SEPARATOR.join(str(r) for r in [k] + v),
                output.items())

            rdd = sc.parallelize(parsed_data, 1)
            rdd.saveAsTextFile(self._get_path(DataType.Domain))

        return output

    def _load_labeled_point_values(self):
        """
        Load valid (possible) values for defined features.

        @return: Possible values for categorical features.
        @rtype: dict
        """
        output = {}

        for record in sc.\
                textFile(self._get_path(DataType.Domain)).\
                map(lambda x: x.split(COL_SEPARATOR)).\
                filter(lambda x: len(x) > 1).\
                collect():

            output[record[0]] = record[1:]

        return output

    @staticmethod
    def _create_labeled_point(record, feature_parameters, valid_values):
        """
        Transforms categorical features in a record to numbers (indices).

        @param record: Data record.
        @type record: list/tuple
        @param feature_parameters: List of feature parameters.
        @type feature_parameters: list/tuple
        @param valid_values: Set of valid (possible) values.
        @type valid_values: dict
        @return: Object with corresponding labeled points.
        @rtype: LabeledPoint
        """
        features = list(record[-len(feature_parameters):])

        for idx, (column_name, column_flag) in enumerate(feature_parameters):
            if column_flag:

                try:
                    features[idx] = float(valid_values[column_name].
                                          index(features[idx]))
                except ValueError:
                    features[idx] = float(len(valid_values[column_name]))

            else:

                if features[idx] in [None, 'None']:
                    features[idx] = 0.
                else:
                    try:
                        features[idx] = float(features[idx])
                    except Exception, e:
                        raise Exception('[Predictor._create_labeled_point] ' +
                                        '{0} ({1}: {2})'.
                                        format(e, column_name, features[idx]))

        return LabeledPoint(record[-len(feature_parameters) - 1] or 0, features)

    def load_model(self):
        """
        Load the model (by using model path based on work_dir).
        """
        self._model = _pa_model.load(sc, self._get_path(DataType.Model))

    def create_model(self):
        """
        Model creation (using training data set).
        """
        # load TRAINING data
        sql_context = SQLContext(sc)
        raw_data = sql_context.read.parquet(self._get_path(DataType.Training))
        filtered_data = raw_data.select(
            [self._config.label_parameter] +
            map(lambda x: x[0], self._config.feature_parameters))

        # generate valid values
        valid_values = self._generate_labeled_point_values(
            data=filtered_data, num_skip=1, save=True)

        # convert categorical features to corresponding indices
        convert_to_labeled_point = self._create_labeled_point
        feature_parameters = self._config.feature_parameters
        training_data = filtered_data.map(lambda x: convert_to_labeled_point(
            record=x,
            feature_parameters=feature_parameters,
            valid_values=valid_values))

        # form parameter categorical_features_info for model creation
        features_names = map(lambda x: x[0], self._config.feature_parameters)
        categorical_features_info = dict(map(
            lambda x: (features_names.index(x), len(valid_values[x]) + 1),
            valid_values))

        try:
            self._model = _pa_method.trainRegressor(
                data=training_data,
                categoricalFeaturesInfo=categorical_features_info,
                **self._config.training_options[_pa_key])
        except ValueError, e:
            raise Exception('[ERROR] Exception in model training: {0}'.
                            format(e))
        else:
            self._model.save(sc, self._get_path(DataType.Model))

    def evaluate_model(self):
        """
        Model evaluation (using test data set).
        """
        # load TEST data
        sql_context = SQLContext(sc)
        raw_data = sql_context.read.parquet(self._get_path(DataType.Test))
        filtered_data = raw_data.select(
            [self._config.label_parameter] +
            map(lambda x: x[0], self._config.feature_parameters))

        # get/load valid values
        valid_values = self._load_labeled_point_values()

        # convert categorical features to corresponding indices
        convert_to_labeled_point = self._create_labeled_point
        feature_parameters = self._config.feature_parameters
        test_data = filtered_data.map(lambda x: convert_to_labeled_point(
            record=x,
            feature_parameters=feature_parameters,
            valid_values=valid_values))

        # calculate predictions
        predictions = self._model.predict(test_data.map(lambda x: x.features))

        labels_with_predictions = (test_data.
                                   map(lambda x: x.label).
                                   zip(predictions))
        try:
            labels_with_predictions.\
                map(lambda x: COL_SEPARATOR.join(str(r) for r in x)).\
                saveAsTextFile('{0}/labels_predictions'.
                               format(self._get_path(DataType.Eval)))
        except:
            pass

        abs_errors = labels_with_predictions.map(lambda (v, p): v - p)
        rel_errors = labels_with_predictions.map(lambda (v, p): (v - p) / v)
        try:
            abs_errors.\
                map(lambda x: COL_SEPARATOR.join(str(r) for r in x)).\
                saveAsTextFile('{0}/abs_errors'.
                               format(self._get_path(DataType.Eval)))
            rel_errors.\
                map(lambda x: COL_SEPARATOR.join(str(r) for r in x)).\
                saveAsTextFile('{0}/rel_errors'.
                               format(self._get_path(DataType.Eval)))
        except:
            pass

    def generate_predictions(self):
        """
        Predictions generation (for input data set).
        """
        # load INPUT data
        sql_context = SQLContext(sc)
        raw_data = sql_context.read.parquet(self._get_path(DataType.Input))
        filtered_data = raw_data.select(
            self._config.label_key_parameters +
            [self._config.label_parameter] +
            map(lambda x: x[0], self._config.feature_parameters))

        # get/load valid values
        valid_values = self._load_labeled_point_values()

        # convert categorical features to corresponding indices
        convert_to_labeled_point = self._create_labeled_point
        feature_parameters = self._config.feature_parameters
        input_data = filtered_data.map(lambda x: convert_to_labeled_point(
            record=x,
            feature_parameters=feature_parameters,
            valid_values=valid_values))

        # calculate predictions
        predictions = self._model.predict(input_data.map(lambda x: x.features))

        num_key_params = len(self._config.label_key_parameters)
        input_key_data = filtered_data.map(lambda x: tuple(x[:num_key_params]))
        key_data_with_predictions = (input_key_data.
                                     zip(predictions).
                                     map(lambda x: x[0] + (x[1],)))

        try:
            key_data_with_predictions.\
                map(lambda x: COL_SEPARATOR.join(str(r) for r in x)).\
                saveAsTextFile(self._get_path(DataType.Output))
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

        if kwargs.get('force'):
            hdfs.remove_dir(self._get_path(DataType.Domain))
            hdfs.remove_dir(self._get_path(DataType.Model))

        self.create_model()

        if with_eval:
            self.run_evaluator(reload_model=False,
                               data_dir=data_dir,
                               force=kwargs.get('force'))

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

        if self._model is None or reload_model:
            self.load_model()

        if kwargs.get('force'):
            hdfs.remove_dir(self._get_path(DataType.Output))

        self.generate_predictions()
