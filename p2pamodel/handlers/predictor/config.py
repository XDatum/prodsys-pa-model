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
import sys

if not os.environ.get('SPARK_HOME'):
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
    sys.path.append(os.environ['SPARK_HOME'])
    os.environ['PYSPARK_PYTHON'] = '/etc/spark/python'

SERVICE_NAME = 'ProdSysPA'

TRAINING_OPTIONS = {
    'gbt': {
        'numIterations': 200,
        'maxDepth': 8,
        'maxBins': 300
    },
    'rf': {
        'numTrees': 75,
        'maxDepth': 8,
        'maxBins': 300,
        'seed': 42
    }
}

LABELED_POINTS = {
    (0, 'project'): [u'data12_8TeV',
                     u'data13_16TeV', u'data13_2p76TeV', u'data13_hip',
                     u'data15_13TeV', u'data15_5TeV', u'data15_comm', u'data15_hi', u'data15_test',
                     u'data16_13TeV', u'data16_cos', u'data16_hip5TeV', u'data16_hip8TeV', u'data16_valid',
                     u'data17_13TeV',
                     u'data_evind',
                     u'mc11_7TeV',
                     u'mc12_14TeV', u'mc12_2TeV', u'mc12_8TeV', u'mc12_valid',
                     u'mc14_13TeV',
                     u'mc15_13TeV', u'mc15_14TeV', u'mc15_5TeV', u'mc15_7TeV', u'mc15_8TeV', u'mc15_pPb8TeV', u'mc15_valid',
                     u'mc16_13TeV', u'mc16_valid',
                     u'mc_evind'],
    (1, 'productionstep'): [u'deriv', u'digit', u'eventIndex', u'evgen', u'merge', u'recon', u'simul', u'skim'],
    (2, 'username'): [u'agbogdan', u'arobson', u'arturos', u'atlas-dpd-production', u'atlas-dpd-production@cern.ch',
                      u'baines', u'bawa', u'bcarlson', u'befreund', u'bernius',
                      u'cchavezb', u'cescobar', u'czodrows',
                      u'damazio', u'default', u'dhirsch', u'dkar', u'dlesny', u'dsouth',
                      u'efeng', u'efullana', u'egramsta', u'eorgill',
                      u'favareto', u'fernando', u'fpastore', u'francav',
                      u'gingrich', u'glushkov', u'goetz',
                      u'htorre',
                      u'iconnell', u'isantoyo',
                      u'jbarkelo', u'jferrand', u'jgarcian', u'jmyers', u'jtanaka', u'jwang', u'jzhong',
                      u'kaplan', u'knikolop', u'kvadla',
                      u'lene',
                      u'mamolla', u'mann', u'maurice', u'mborodin', u'mcfayden', u'mdobre', u'mehlhase', u'mhodgkin', u'miochoa', u'mnegrini', u'mshapiro', u'mughetto', u'mwielers',
                      u'nmagini',
                      u'olszewsk',
                      u'pacheco', u'pagacova',
                      u'retmas', u'rkeyes', u'rnayyar',
                      u'sgeorge', u'sgonzale', u'shuli', u'smazza', u'sthenkel',
                      u'tnobe', u'toyu', u'tvazquez',
                      u'vanyash',
                      u'wanghill', u'wguan', u'wyswys',
                      u'ycoadou', u'ykeisuke',
                      u'zgrout'],
    (3, 'workinggroup'): [u'AP_BPHY',
                          u'AP_DAPR',
                          u'AP_EGAM', u'AP_EXOT',
                          u'AP_FTAG',
                          u'AP_HIGG', u'AP_HION',
                          u'AP_IDET', u'AP_IDTR',
                          u'AP_JETM',
                          u'AP_MCGN', u'AP_MUON',
                          u'AP_PHYS',
                          u'AP_REPR',
                          u'AP_SIMU', u'AP_SOFT', u'AP_STDM', u'AP_SUSY',
                          u'AP_TAUP', u'AP_TDAQ', u'AP_THLT', u'AP_TOPQ', u'AP_TRIG',
                          u'AP_UPGR',
                          u'AP_VALI',
                          u'GP_BPHY',
                          u'GP_EGAM', u'GP_EXOT',
                          u'GP_FTAG',
                          u'GP_HIGG', u'GP_HION',
                          u'GP_IDTR',
                          u'GP_JETM',
                          u'GP_MCGN', u'GP_MUON',
                          u'GP_PHYS',
                          u'GP_REPR',
                          u'GP_SOFT', u'GP_STDM', u'GP_SUSY',
                          u'GP_TAUP', u'GP_THLT', u'GP_TOPQ', u'GP_TRIG',
                          u'GP_UPGR',
                          u'GP_VALI'],
    (4, 'prodsourcelabel'): [u'managed', u'ptest'],
    (5, 'processingtype'): [u'deriv', u'digit',
                            u'eventIndex', u'evgen',
                            u'merge',
                            u'overlay',
                            u'pile',
                            u'recon', u'reprocessing',
                            u'simul', u'skim',
                            u'urgent',
                            u'validation'],
    (6, 'architecture'): [u'i686-slc5-gcc43-opt',
                          u'x86_64-slc5-gcc43-opt',
                          u'x86_64-slc6-gcc4.7', u'x86_64-slc6-gcc46-opt', u'x86_64-slc6-gcc47-opt', u'x86_64-slc6-gcc48-opt', u'x86_64-slc6-gcc49-opt',
                          u'x86_64-slc6-gcc62-dbg', u'x86_64-slc6-gcc62-opt'],
    (7, 'transpath'): [u'AODMerge_tf.py', u'Archive_tf.py', u'AtlasG4_tf.py', u'AtlasG4_trf.py',
                       u'BSOverlayFilter_tf.py',
                       u'DigiMReco_trf.py', u'Digi_tf.py',
                       u'ESDMerge_tf.py', u'EVNTMerge_tf.py',
                       u'FilterHit_tf.py', u'FilterHit_trf.py',
                       u'Generate_tf.py', u'Generate_trf.py',
                       u'HISTMerge_tf.py', u'HITSMerge_tf.py', u'HLTHistMerge_tf.py',
                       u'Merging_trf.py',
                       u'NTUPMerge_tf.py',
                       u'OverlayChain_tf.py',
                       u'POOLtoEI_tf.py',
                       u'RAWMerge_tf.py', u'Reco_tf.py', u'Reco_trf.py',
                       u'Sim_tf.py',
                       u'TrigFTKTM32SM1Un_tf.py', u'TrigFTKTM64SM1Un_tf.py',
                       u'TrigFTKTM64SM4Un_tf.py', u'Trig_reco_tf.py'],
    (8, 'transuses'): [u'Atlas-14.2.24',
                       u'Atlas-16.6.7',
                       u'Atlas-17.0.6',
                       u'Atlas-17.2.0', u'Atlas-17.2.1', u'Atlas-17.2.10', u'Atlas-17.2.11', u'Atlas-17.2.13', u'Atlas-17.2.14',
                       u'Atlas-17.2.4', u'Atlas-17.2.5', u'Atlas-17.2.6', u'Atlas-17.2.7', u'Atlas-17.2.8',
                       u'Atlas-17.3.12', u'Atlas-17.3.13',
                       u'Atlas-17.7.0', u'Atlas-17.7.3',
                       u'Atlas-19.0.3',
                       u'Atlas-19.2.0', u'Atlas-19.2.1', u'Atlas-19.2.3', u'Atlas-19.2.4', u'Atlas-19.2.5', u'Atlas-2.3.48',
                       u'Atlas-2.6', u'Atlas-2.6.2', u'Atlas-2.6.3', u'Atlas-2.6.4',
                       u'Atlas-20.1.0', u'Atlas-20.1.4', u'Atlas-20.1.5', u'Atlas-20.1.6', u'Atlas-20.1.7', u'Atlas-20.1.8', u'Atlas-20.1.9',
                       u'Atlas-20.20.10',
                       u'Atlas-20.20.3', u'Atlas-20.20.5', u'Atlas-20.20.6', u'Atlas-20.20.7', u'Atlas-20.20.8', u'Atlas-20.20.9',
                       u'Atlas-20.3.3', u'Atlas-20.3.7',
                       u'Atlas-20.7.1', u'Atlas-20.7.3', u'Atlas-20.7.4', u'Atlas-20.7.5', u'Atlas-20.7.6', u'Atlas-20.7.7', u'Atlas-20.7.8', u'Atlas-20.7.9',
                       u'Atlas-20.8.2',
                       u'Atlas-2017',
                       u'Atlas-21.0', u'Atlas-21.0-TrigMC', u'Atlas-21.0.0',
                       u'Atlas-21.0.10', u'Atlas-21.0.11', u'Atlas-21.0.12', u'Atlas-21.0.13', u'Atlas-21.0.14', u'Atlas-21.0.15', u'Atlas-21.0.16', u'Atlas-21.0.17', u'Atlas-21.0.18', u'Atlas-21.0.19',
                       u'Atlas-21.0.20', u'Atlas-21.0.21', u'Atlas-21.0.22', u'Atlas-21.0.23', u'Atlas-21.0.24', u'Atlas-21.0.25', u'Atlas-21.0.26', u'Atlas-21.0.27', u'Atlas-21.0.28', u'Atlas-21.0.29',
                       u'Atlas-21.0.30', u'Atlas-21.0.31', u'Atlas-21.0.32', u'Atlas-21.0.33', u'Atlas-21.0.34', u'Atlas-21.0.35', u'Atlas-21.0.36', u'Atlas-21.0.37', u'Atlas-21.0.38', u'Atlas-21.0.39',
                       u'Atlas-21.0.8',
                       u'Atlas-21.1', u'Atlas-21.1-dev',
                       u'Atlas-21.1.10', u'Atlas-21.1.11', u'Atlas-21.1.13',
                       u'Atlas-21.1.2', u'Atlas-21.1.3', u'Atlas-21.1.4', u'Atlas-21.1.5', u'Atlas-21.1.6', u'Atlas-21.1.8', u'Atlas-21.1.9',
                       u'Atlas-21.2', u'Atlas-21.2.0', u'Atlas-21.2.1', u'Atlas-21.2.2', u'Atlas-21.2.2.0', u'Atlas-21.2.3', u'Atlas-21.2.3.0', u'Atlas-21.2.4', u'Atlas-21.2.5', u'Atlas-21.2.6',
                       u'Atlas-21.3', u'Atlas-21.3.0',
                       u'Atlas-21.5.0', u'Atlas-21.5.1',
                       u'Atlas-AnalysisBase-2.6.X',
                       u'Atlas-local/simulation/21.0',
                       u'Atlas-master'],
    (9, 'cloud'): [u'CERN', u'WORLD'],
    (10, 'ramunit'): [u'MB', u'MBPerCore', u'MBPerCoreFixed'],
    (11, 'corecount'): [u'0', u'1', u'4', u'8', u'9', u'16']
}

# (!) should be the same as in parquet-file (after parquet-converter)
SELECT_PARAMS = [
    'DURATION',
    'PROJECT',
    'PRODUCTIONSTEP',
    'USERNAME',
    'WORKINGGROUP',
    'PRODSOURCELABEL',
    'PROCESSINGTYPE',
    'ARCHITECTURE',
    'TRANSPATH',
    'TRANSUSES',
    'CLOUD',
    'RAMUNIT',
    'CORECOUNT',
    'RAMCOUNT',
    'WEEKDAY'
]
