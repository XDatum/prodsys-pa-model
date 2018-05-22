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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2018
#

import importlib

SETTINGS_MODULE = 'p2pamodel.config.settings'


class Settings(object):

    def __init__(self, settings_module=None):
        settings_module = settings_module or SETTINGS_MODULE
        if not settings_module:
            raise Exception('[P2PAMODEL] Settings module is not defined.')

        mod = importlib.import_module(settings_module)
        for setting in dir(mod):
            setattr(self, setting, getattr(mod, setting))


settings = Settings()
