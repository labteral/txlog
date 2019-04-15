#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rocksdb
from . import utils
import logging
import pickle


class Record:
    def __init__(self, value=None, record_bytes=None):
        if record_bytes != None:
            self.value, self.timestamp = \
                Record.get_values_from_record_bytes(record_bytes)
            return
        self.value = value
        self.timestamp = utils.get_timestamp_ms()
        

class TxLog:

    def __init__(self, chain_dir='./txlog_data', signature_checker=None):
        self._db = rocksdb.DB(f'{chain_dir}', rocksdb.Options(create_if_missing=True))
        self._write_batch = None

    def begin_transaction(self):            
        self._write_batch = rocksdb.WriteBatch()

    def commit(self):
        self._db.write(self._write_batch)
        self._write_batch = None

    def get(self, index):
        return self._get(index, prefix='txlog_')

    def get_latest(self):
        index = self._get_index()
        return self._get(index, prefix='txlog_')
    
    def put(self, value):
        self.begin_transaction()
        index = self._increment_index()
        self._put(index, value, prefix='txlog_')
        self.commit()      
            
    def _get(self, key, prefix=''):
        key_bytes = utils.to_bytes(f'{prefix}{key}')
        value = self._db.get(key_bytes)
        if value != None:
            return pickle.loads(value)

    def _put(self, key, value, prefix=''):
        key = utils.to_bytes(f'{prefix}{key}')
        value = pickle.dumps(Record(value), protocol=4)
        if self._write_batch:
            self._write_batch.put(key, value)
        else:
            self._db.put(key, value, sync=True)        

    def _increment_index(self):
        index = self._get_index() + 1
        self._put('index', index, prefix='meta')
        return index

    def _get_index(self):
        record = self._get('index', prefix='meta')
        if record != None:
            return int(record.value)
        return -1