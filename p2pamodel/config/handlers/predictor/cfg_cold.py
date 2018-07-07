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

from ..configbase import ConfigBase


config = ConfigBase('Config for cold-prediction')

config.training_options = {
    'gbt': {'numIterations': 200,
            'maxDepth': 8,
            'maxBins': 300},
    'rf': {'numTrees': 75,
           'maxDepth': 8,
           'maxBins': 300,
           'seed': 42}
}

# - (!) same as in [parquet] converter -
config.label_key_parameters = ['TASKID', 'SUBMITTIME']
config.label_parameter = 'DURATION'
config.feature_parameters = [
    ('PROJECT', 1),
    ('PRODUCTIONSTEP', 1),
    ('USERNAME', 1),
    ('WORKINGGROUP', 1),
    ('PRODSOURCELABEL', 1),
    ('PROCESSINGTYPE', 1),
    ('ARCHITECTURE', 1),
    ('TRANSPATH', 1),
    ('TRANSUSES', 1),
    ('CORECOUNT', 1),
    ('RAMCOUNT', 0),
    ('RAMUNIT', 1),
    ('PRIORITY', 0),
    ('OUTPUTFORMATS', 1)
]
