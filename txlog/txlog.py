#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rocksdb
from . import utils
import logging
from pickle5 import pickle


class Transaction:
    def __init__(self, method, args=None, kwargs=None):
        self.creation_timestamp = utils.get_timestamp_ms()
        self.commitment_timestamp = None
        self.committed = False

        self.method = method
        if type(args) != list:
            self.args = [args]
        else:
            self.args = args
        self.kwargs = kwargs

    def exec(self, container_object):
        if self.args:
            getattr(container_object, self.method)(*self.args)
        elif self.kwargs:
            getattr(container_object, self.method)(**self.kwargs)
        else:
            getattr(container_object, self.method)()

    def _set_index(self, index):
        self.index = index


class TxLog:
    # 7 days = 604800 seconds
    def __init__(self, txlog_dir='./txlog_data', signature_checker=None, circular=True, min_age=604800):
        self._db = rocksdb.DB(f'{txlog_dir}', rocksdb.Options(create_if_missing=True))
        self._write_batch = None
        self._min_age = min_age

    def begin_write_batch(self):
        if self._write_batch == None:
            self._write_batch = rocksdb.WriteBatch()

    def commit_write_batch(self):
        if self._write_batch != None:
            self._db.write(self._write_batch, sync=True)
            self._write_batch = None

    def commit(self, index):
        tx = self._get(index, prefix='txlog_')
        if tx == None:
            raise IndexError
        assert (self._get_committed_offset() == index - 1)
        tx.committed = True
        tx.commitment_timestamp = utils.get_timestamp_ms()
        self._update_tx(index, tx)

    def get(self, index):
        return self._get(index, prefix='txlog_')

    def exec_uncommitted_txs(self, container_object):
        """
        Executes all pending transactions. The methods for all the uncommitted 
        transactions should be available in the container object
        """
        for tx in self.get_uncommitted_txs():
            tx.exec(container_object)
            self.commit(tx.index)

    def get_latest_uncommitted_tx(self):
        if self._get_tx_offset() < self._get_committed_offset() + 1:
            return
        return self._get(self._get_committed_offset() + 1, prefix='txlog_')

    def put(self, tx):
        if type(tx) != Transaction:
            raise TypeError
        index = self._get_tx_offset() + 1
        tx._set_index(index)
        self._put_tx(index, tx)
        self._truncate()

    def get_txs(self):
        iterator = self._db.iteritems()
        iterator.seek(b'txlog_')
        for _, tx in iterator:
            yield pickle.loads(tx)

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
        for key, tx in iterator:
            tx = pickle.loads(tx)
            if tx.creation_timestamp < utils.get_timestamp_ms() - self._min_age * 1000:
                keys_to_delete.append(key)
                continue
            break
        for key in keys_to_delete:
            self._db.delete(key)

    def _get(self, key, prefix=''):
        key_bytes = utils.to_bytes(f'{prefix}{key}')
        tx = self._db.get(key_bytes)
        if tx != None:
            return pickle.loads(tx)

    def _put(self, key, value, prefix=''):
        key = utils.to_bytes(f'{prefix}{key}')
        value_bytes = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        if self._write_batch != None:
            self._write_batch.put(key, value_bytes)
        else:
            self._db.put(key, value_bytes, sync=True)

    def _update_tx(self, index, tx):
        if type(tx) != Transaction:
            raise TypeError
        self._put(index, tx, prefix='txlog_')

    def _put_tx(self, index, tx):
        new_batch = False
        if self._write_batch == None:
            self.begin_write_batch()
            new_batch = True

        if type(tx) != Transaction:
            raise TypeError
        self._put(index, tx, prefix='txlog_')
        self._increment_committed_offset()

        if new_batch:
            self.commit_write_batch()

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
        self._put(attribute, index, prefix='meta')

    def _get_offset_attribute(self, attribute):
        value = self._get(attribute, prefix='meta')
        if value != None:
            return value
        return -1