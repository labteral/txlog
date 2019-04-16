#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rocksdb
from . import utils
import logging
import pickle


class Record:
    def __init__(self, value, index=None):
        self.value = value
        self.index = index
        self.creation_timestamp = utils.get_timestamp_ms()
        self.commitment_timestamp = None
        self.committed = False


class TxLog:
    # Default to 7 days
    def __init__(self, chain_dir='./txlog_data', signature_checker=None, circular=True, min_age=604800):
        self._db = rocksdb.DB(f'{chain_dir}', rocksdb.Options(create_if_missing=True))
        self._write_batch = None
        self._min_age = min_age

    def begin_write_batch(self):
        self._write_batch = rocksdb.WriteBatch()

    def commit_write_batch(self):
        self._db.write(self._write_batch, sync=True)
        self._write_batch = None

    def commit(self, index):
        record = self._get(index, prefix='txlog_')
        if record == None:
            raise IndexError
        assert (self._get_committed_offset() == index - 1)
        record.committed = True
        record.commitment_timestamp = utils.get_timestamp_ms()
        self.begin_write_batch()
        self._put(index, record)
        self._increment_committed_offset()
        self.commit_write_batch()

    def get(self, index):
        return self._get(index, prefix='txlog_')

    def get_latest_uncommitted_tx(self):
        if self._get_tx_offset() < self._get_committed_offset() + 1:
            return
        return self._get(self._get_committed_offset() + 1, prefix='txlog_')

    def put(self, value):
        index = self._get_tx_offset() + 1
        record = Record(value, index)

        self.begin_write_batch()
        self._increment_tx_offset()
        self._put(index, record, prefix='txlog_')
        self.commit_write_batch()

        self._truncate()

    def get_txs(self):
        iterator = self._db.iteritems()
        iterator.seek(b'txlog_')
        for _, value in iterator:
            yield pickle.loads(value)

    def get_uncommitted_txs(self):
        latest_uncommitted_tx = self.get_latest_uncommitted_tx()
        if latest_uncommitted_tx != None:
            for index in range(latest_uncommitted_tx.index, self._get_tx_offset() + 1):
                yield self.get(index)

    def _truncate(self):
        if not self._min_age:
            return
        iterator = self._db.iteritems()
        iterator.seek(b'txlog_')
        keys_to_delete = []
        for key, value in iterator:
            record = pickle.loads(value)
            if record.creation_timestamp < utils.get_timestamp_ms() - self._min_age * 1000:
                keys_to_delete.append(key)
                continue
            break
        for key in keys_to_delete:
            self._db.delete(key)

    def _get(self, key, prefix=''):
        key_bytes = utils.to_bytes(f'{prefix}{key}')
        value = self._db.get(key_bytes)
        if value != None:
            return pickle.loads(value)

    def _put(self, key, value, prefix=''):
        if type(value) != Record:
            raise TypeError
        key = utils.to_bytes(f'{prefix}{key}')
        bytes_value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        if self._write_batch != None:
            self._write_batch.put(key, bytes_value)
        else:
            self._db.put(key, bytes_value, sync=True)

    def _increment_committed_offset(self):
        self._increment_offset_attribute('committed_index')

    def _get_committed_offset(self):
        return self._get_offset_attribute('committed_index')

    def _increment_tx_offset(self):
        self._increment_offset_attribute('index')

    def _get_tx_offset(self):
        return self._get_offset_attribute('index')

    def _increment_offset_attribute(self, attribute):
        index = self._get_offset_attribute(attribute) + 1
        record = Record(index)
        self._put(attribute, record, prefix='meta')

    def _get_offset_attribute(self, attribute):
        record = self._get(attribute, prefix='meta')
        if record != None:
            return int(record.value)
        return -1