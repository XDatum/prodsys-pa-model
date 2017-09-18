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
    (2, 'provenance'): [u'AP', u'GP'],
    (3, 'username'): [u'agbogdan', u'arobson', u'arturos', u'atlas-dpd-production',
                      u'baines', u'bawa', u'bcarlson', u'befreund', u'bernius',
                      u'cchavezb', u'cescobar', u'czodrows',
                      u'damazio', u'default', u'dhirsch', u'dkar', u'dlesny', u'dsouth',
                      u'efeng', u'efullana', u'egramsta',
                      u'favareto', u'fernando', u'fpastore', u'francav',
                      u'gingrich', u'glushkov', u'goetz',
                      u'htorre',
                      u'iconnell', u'isantoyo',
                      u'jbarkelo', u'jferrand', u'jgarcian', u'jmyers', u'jtanaka', u'jwang', u'jzhong',
                      u'kaplan', u'knikolop', u'kvadla',
                      u'mamolla', u'mann', u'maurice', u'mborodin', u'mcfayden', u'mdobre', u'mehlhase', u'mhodgkin', u'miochoa', u'mshapiro', u'mughetto', u'mwielers',
                      u'olszewsk',
                      u'pacheco', u'pagacova',
                      u'retmas', u'rkeyes', u'rnayyar',
                      u'sgeorge', u'sgonzale', u'shuli', u'smazza', u'sthenkel',
                      u'toyu', u'tvazquez',
                      u'vanyash',
                      u'wanghill', u'wguan',
                      u'ycoadou', u'ykeisuke',
                      u'zgrout'],
    (4, 'workinggroup'): [u'AP_BPHY',
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
    (5, 'processingtype'): [u'deriv', u'digit',
                            u'eventIndex', u'evgen',
                            u'merge',
                            u'overlay',
                            u'pile',
                            u'recon', u'reprocessing',
                            u'simul', u'skim',
                            u'urgent',
                            u'validation'],
    (6, 'cloud'): [u'CERN', u'WORLD'],
    (7, 'prodsourcelabel'): [u'managed', u'ptest'],
    (8, 'corecount'): [u'0', u'1', u'4', u'8', u'9'],
    (9, 'architecture'): [u'i686-slc5-gcc43-opt',
                          u'x86_64-slc5-gcc43-opt',
                          u'x86_64-slc6-gcc47-opt', u'x86_64-slc6-gcc48-opt', u'x86_64-slc6-gcc49-opt', u'x86_64-slc6-gcc62-opt'],
    (10, 'transpath'): [u'AODMerge_tf.py', u'Archive_tf.py', u'AtlasG4_tf.py', u'AtlasG4_trf.py',
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
                        u'TrigFTKTM32SM1Un_tf.py', u'TrigFTKTM64SM1Un_tf.py', u'TrigFTKTM64SM4Un_tf.py', u'Trig_reco_tf.py'],
    (11, 'nucleus'): [u'AGLT2', u'Australia-ATLAS',
                      u'BNL-ATLAS', u'BNL_PROD', u'BU_ATLAS_Tier2',
                      u'CERN-PROD',
                      u'DESY-HH', u'DESY-ZN',
                      u'FZK-LCG2',
                      u'IFIC-LCG2', u'IN2P3-CC', u'IN2P3-LAPP', u'INFN-NAPOLI-ATLAS', u'INFN-ROMA1', u'INFN-T1',
                      u'LRZ-LMU',
                      u'MPPMU', u'MWT2',
                      u'NDGF-T1', u'NIKHEF-ELPROD',
                      u'RAL-LCG2', u'RAL-LCG2-ECHO', u'RRC-KI-T1',
                      u'SARA-MATRIX', u'SLAC', u'SWT2_CPB',
                      u'TOKYO-LCG2', u'TRIUMF-LCG2', u'Taiwan-LCG2',
                      u'UAM-LCG2', u'UKI-LT2-QMUL', u'UKI-NORTHGRID-LANCS-HEP', u'UKI-NORTHGRID-MAN-HEP', u'UKI-SCOTGRID-GLASGOW', u'UNI-FREIBURG',
                      u'pic', u'praguelcg2',
                      u'wuppertalprod'],
    (12, 'workqueue_id'): [u'1', u'2', u'3', u'4', u'7', u'8', u'9',
                           u'10', u'12', u'13', u'14', u'15',
                           u'20',
                           u'200', u'201',
                           u'1000', u'1001', u'1002', u'1003', u'1004', u'1010'],
    (13, 'gshare'): [u'Data Derivations',
                     u'Event Index',
                     u'Group production',
                     u'HLT Reprocessing', u'Heavy Ion',
                     u'MC 16', u'MC 16 evgen', u'MC Default', u'MC Default evgen', u'MC Derivations', u'MC Production', u'MC production',
                     u'Overlay',
                     u'Reprocessing default',
                     u'Test',
                     u'Upgrade',
                     u'Validation']
}

# (!) should be the same as in parquet-file (after parquet-converter)
SELECT_PARAMS = [
    'DURATION',
    'PROJECT',
    'PRODUCTIONSTEP',
    'PROVENANCE',
    'USERNAME',
    'WORKINGGROUP',
    'PROCESSINGTYPE',
    'CLOUD',
    'PRODSOURCELABEL',
    'CORECOUNT',
    'ARCHITECTURE',
    'TRANSPATH',
    'NUCLEUS',
    'WORKQUEUE_ID',
    'GSHARE',
    'TASKPRIORITY',
    'WEEKDAY',
    'WEEK',
    'TOTAL_REQ_JOBS',
    'WALLTIME',
    'RAMCOUNT'
]
