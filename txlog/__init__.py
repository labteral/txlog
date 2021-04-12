#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .txlog import TxLog, Call
import logging

__version__ = '2.214.0'

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
