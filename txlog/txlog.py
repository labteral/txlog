#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rocksdb
from rocksdb import DB, WriteBatch, Options
from . import utils
from pickle5 import pickle

# TODO RESET INDEX % 1M
# pensar la comparación del assert cuando se haga el index circular
# el momento crítico es al volver a empezar


class Transaction:
    def __init__(self, method, args=None, kwargs=None):
        self._creation_timestamp = utils.get_timestamp_ms()
        self._commitment_timestamp = None
        self._committed = False

        self._method = method
        if type(args) != list:
            self._args = [args]
        else:
            self._args = args
        self._kwargs = kwargs
        self._index = None

    @property
    def creation_timestamp(self):
        return self._creation_timestamp

    @property
    def commitment_timestamp(self):
        return self._commitment_timestamp

    @property
    def committed(self):
        return self._committed

    @property
    def method(self):
        return self._method

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def index(self):
        return self._index

    def exec(self, container_object=None):
        if container_object is None:
            try:
                method = globals()[self._method]
            except KeyError:
                import builtins
                method = getattr(builtins, self._method)
        else:
            method = getattr(container_object, self._method)

        if self._args is not None:
            method(*self._args)
        elif self._kwargs is not None:
            method(**self._kwargs)
        else:
            method()

    def set_index(self, index):
        self._index = index


class TxLog:
    def __init__(self, txlog_dir='./txlog_data', signature_checker=None, circular=True, min_age=604800):
        # 7 days = 604800 seconds
        self._write_batch = None
        self._min_age = min_age
        self._db = DB(f'{txlog_dir}', Options(create_if_missing=True))

    def begin_write_batch(self):
        if self._write_batch is None:
            self._write_batch = WriteBatch()

    def commit_write_batch(self):
        if self._write_batch is not None:
            self._db.write(self._write_batch, sync=True)
            self._write_batch = None

    def commit(self, index):
        tx = self._get(index, prefix='txlog_')
        if tx is None:
            raise IndexError
        assert (self._get_offset() == index - 1)
        tx._committed = True
        tx._commitment_timestamp = utils.get_timestamp_ms()
        self._update_tx(index, tx)
        self._increment_offset()

    def get(self, index):
        return self._get(index, prefix='txlog_')

    def exec_uncommitted_txs(self, container_object=None):
        """
        Executes all pending transactions. The methods for all the uncommitted 
        transactions should be available in the container object
        """
        for tx in self.get_uncommitted_txs():
            tx.exec(container_object)
            self.commit(tx.index)

    def get_latest_uncommitted_tx(self):
        next_offset = self._get_next_offset()
        if self._get_index() < next_offset:
            return
        return self._get(next_offset, prefix='txlog_')

    def put(self, tx):
        if type(tx) != Transaction:
            raise TypeError
        index = self._get_next_index()
        tx.set_index(index)
        self._put_tx(index, tx)
        self._truncate()

    def get_txs(self):
        iterator = self._db.iteritems()
        iterator.seek(b'txlog_')
        for _, tx in iterator:
            yield pickle.loads(tx)

    def get_uncommitted_txs(self):
        latest_uncommitted_tx = self.get_latest_uncommitted_tx()
        if latest_uncommitted_tx is not None:
            for index in range(latest_uncommitted_tx.index, self._get_next_index()):
                yield self.get(index)

    def _truncate(self):
        if self._min_age is None:
            return
        iterator = self._db.iteritems()
        iterator.seek(b'txlog_')
        keys_to_delete = []
        for key, tx in iterator:
            tx = pickle.loads(tx)
            if tx._creation_timestamp < utils.get_timestamp_ms() - self._min_age * 1000:
                keys_to_delete.append(key)
                continue
            break
        for key in keys_to_delete:
            self._db.delete(key)

    def _get(self, key, prefix=''):
        key_bytes = utils.to_bytes(f'{prefix}{key}')
        tx = self._db.get(key_bytes)
        if tx is not None:
            return pickle.loads(tx)

    def _put(self, key, value, prefix=''):
        key_bytes = utils.to_bytes(f'{prefix}{key}')
        value_bytes = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        if self._write_batch is not None:
            self._write_batch.put(key_bytes, value_bytes)
        else:
            self._db.put(key_bytes, value_bytes, sync=True)

    def _update_tx(self, index, tx):
        if type(tx) != Transaction:
            raise TypeError
        self._put(index, tx, prefix='txlog_')

    def _put_tx(self, index, tx):
        is_batch_new = False
        if self._write_batch is None:
            self.begin_write_batch()
            is_batch_new = True

        if type(tx) != Transaction:
            raise TypeError
        self._put(index, tx, prefix='txlog_')
        self._increment_index()

        if is_batch_new:
            self.commit_write_batch()

    def _increment_offset(self):
        self._increment_int_attribute('offset')

    def _get_offset(self):
        return self._get_int_attribute('offset')

    def _get_next_offset(self):
        return self._get_int_attribute('offset') + 1

    def _increment_index(self):
        self._increment_int_attribute('index')

    def _get_index(self):
        return self._get_int_attribute('index')

    def _get_next_index(self):
        return self._get_int_attribute('index') + 1

    def _increment_int_attribute(self, attribute):
        index = self._get_int_attribute(attribute) + 1
        self._put(attribute, index, prefix='meta')

    def _get_int_attribute(self, attribute):
        value = self._get(attribute, prefix='meta')
        if value is not None:
            return value
        else:
            return -1
