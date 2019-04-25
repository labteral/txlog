#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .txlog import TxLog, Transaction
import logging

__version__ = '0.0.0.10a'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.info(f'TxLog v{__version__}')
