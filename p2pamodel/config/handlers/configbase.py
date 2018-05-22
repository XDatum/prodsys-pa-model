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


class ConfigBase(object):

    def __init__(self, *args):
        self.__header__ = str(args[0]) if args else None

    def __repr__(self):
        if self.__header__ is None:
            return super(ConfigBase, self).__repr__()
        return self.__header__

    @property
    def __config_keys__(self):
        return filter(
            lambda x: not (x.startswith('__') or isinstance(x, ConfigBase)),
            self.__dict__.keys())

    def next(self):
        raise StopIteration

    def __iter__(self):
        for key in self.__config_keys__:
            yield getattr(self, key)

    def __len__(self):
        return len(self.__config_keys__)
