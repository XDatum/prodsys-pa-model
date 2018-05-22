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

from ..utils import EnumTypes


DataType = EnumTypes(
    ('Training', 'training'),
    ('Test', 'test'),
    ('Model', 'model'),
    ('Eval', 'eval'),
    ('Input', 'input'),
    ('Output', 'output'),
    ('Domain', 'domain'),
)

DirType = EnumTypes(
    ('Work', 'work'),
    ('Data', 'data')
)
