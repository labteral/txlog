#!/usr/bin/env python
# -*- coding: utf-8 -*-

from easyrocks import DB, WriteBatch, utils
import logging
import time

# TODO RESET INDEX % 1M
# pensar la comparación del assert cuando se haga el index circular
# el momento crítico es al volver a empezar


def get_timestamp_ms():
    return int(round(time.time() * 1000))


class Transaction:
    def __init__(self, method, args=None, kwargs=None):
        self._creation_timestamp = get_timestamp_ms()
        self._commitment_timestamp = None
        self._committed = False

        self._method = method

        if args is None:
            self._args = []
        elif isinstance(args, list):
            self._args = args
        else:
            self._args = [args]

        if kwargs is None:
            self._kwargs = {}
        else:
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

        method(*self._args, **self._kwargs)

    def set_index(self, index):
        self._index = index


class TxLog:
    def __init__(self, txlog_dir='./txlog_data', signature_checker=None, circular=True, min_age=604800):
        # 7 days = 604800 seconds
        self._batch_index = None
        self._write_batch = None
        self._min_age = min_age
        self._db = DB(f'{txlog_dir}')

    def begin_write_batch(self):
        if self._write_batch is None:
            self._batch_index = self._get_index()
            self._write_batch = WriteBatch()

    def commit_write_batch(self):
        if self._write_batch is not None:
            self._db.write(self._write_batch, sync=True)
            self._write_batch = None

    def destroy_write_batch(self):
        self._write_batch = None

    def commit(self, index):
        tx = self._db.get(f'txlog_{utils.get_padded_int(index)}')
        if tx is None:
            raise IndexError
        assert (self._get_offset() == index - 1)
        tx._committed = True
        tx._commitment_timestamp = get_timestamp_ms()
        self._update_tx(index, tx)
        self._increment_offset()

    def get(self, index):
        return self._db.get(f'txlog_{utils.get_padded_int(index)}')

    def exec_uncommitted_txs(self, container_object=None):
        """
        Executes all pending transactions. The methods for all the uncommitted 
        transactions should be available in the container object
        """
        for tx in self.get_uncommitted_txs():
            tx.exec(container_object)
            self.commit(tx.index)

    def put(self, tx):
        if not isinstance(tx, Transaction):
            raise TypeError
        index = self._get_next_index()
        tx.set_index(index)
        self._put_tx(index, tx)
        self._truncate()

    def get_txs(self):
        for _, tx in self._db.scan(prefix='txlog_'):
            yield tx

    def get_first_uncommitted_tx(self):
        next_offset = self._get_next_offset()
        if self._get_index() < next_offset:
            return
        return self._db.get(f'txlog_{utils.get_padded_int(next_offset)}')

    def get_uncommitted_txs(self):
        first_uncommitted_tx = self.get_first_uncommitted_tx()
        if first_uncommitted_tx is not None:
            for index in range(first_uncommitted_tx.index, self._get_next_index()):
                yield self.get(index)

    def _truncate(self):
        if self._min_age is None:
            return

        keys_to_delete = []
        for key, tx in self._db.scan(prefix='txlog_'):
            if tx._creation_timestamp < get_timestamp_ms() - self._min_age * 1000:
                keys_to_delete.append(key)
                continue
            break
        for key in keys_to_delete:
            self._db.delete(key)

    def _update_tx(self, index, tx):
        if not isinstance(tx, Transaction):
            raise TypeError
        self._db.put(f'txlog_{utils.get_padded_int(index)}', tx)

    def _put_tx(self, index, tx):
        if not isinstance(tx, Transaction):
            raise TypeError

        is_batch_new = False
        if self._write_batch is None:
            is_batch_new = True
            self.begin_write_batch()

        self._db.put(f'txlog_{utils.get_padded_int(index)}', tx)
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
        if self._write_batch is not None:
            return self._batch_index
        return self._get_int_attribute('index')

    def _get_next_index(self):
        if self._write_batch is not None:
            return self._batch_index + 1
        return self._get_int_attribute('index') + 1

    def _increment_int_attribute(self, attribute):
        if attribute == 'index' and self._write_batch is not None:
            self._batch_index += 1
            index = self._batch_index
        else:
            index = self._get_int_attribute(attribute) + 1
        self._db.put(f'meta_{attribute}', index)

    def _get_int_attribute(self, attribute):
        value = self._db.get(f'meta_{attribute}')
        if value is not None:
            return int(value)
        else:
            return -1
