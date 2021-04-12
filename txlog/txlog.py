#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .utils import get_timestamp_ms
from easyrocks import RocksDB, WriteBatch, utils
import builtins
from typing import Generator

UINT_BYTES = 5
MAX_UINT = 2**(UINT_BYTES * 8) - 1


class Call:
    def __init__(self, method, args=None, kwargs=None):
        self._creation_timestamp = get_timestamp_ms()
        self._commitment_timestamp = None
        self._committed = False

        self._method_name = method

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
        return self._method_name

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
                method = globals()[self._method_name]
            except KeyError:
                try:
                    method = getattr(builtins, self._method_name)
                except KeyError:
                    raise ValueError('Method not found')
        elif isinstance(container_object, dict):
            method = container_object[self._method_name]
        else:
            method = getattr(container_object, self._method_name)

        method(*self._args, **self._kwargs)

    def set_index(self, index):
        self._index = index


class TxLog:

    CALL_PREFIX = b'\x00'
    OFFSET_PREFIX = b'\x01'
    INDEX_PREFIX = b'\x02'
    META_PREFIX = b'\x03'

    def __init__(self,
                 path='./txlog-data',
                 max_committed_items=0,
                 committed_ttl_seconds=None):
        self._batch_index = None
        self._write_batch = None
        self._committed_ttl_seconds = committed_ttl_seconds
        self._max_committed_items = max_committed_items
        self._db = RocksDB(f'{path}')

    def begin(self):
        if self._write_batch is None:
            self._batch_index = self._get_index()
            self._write_batch = WriteBatch()

    def commit(self):
        self._db.commit(self._write_batch)
        self._write_batch = None

    def rollback(self):
        self._write_batch = None

    def commit_call(self, call: Call):
        is_new_write_batch = self._write_batch is None
        if is_new_write_batch:
            self.begin()

        call._committed = True
        call._commitment_timestamp = get_timestamp_ms()
        self._update_call(call.index, call)
        self._increment_offset()

        if is_new_write_batch:
            self.commit()

    def get(self, index: int) -> Call:
        call_key = self._get_call_key(index)
        return self._db.get(call_key)

    def exec_uncommitted_calls(self, container_object=None):
        for call in self.get_uncommitted_calls():
            call.exec(container_object)
            self.commit_call(call)

    def add(self, call: Call):
        if not isinstance(call, Call):
            raise TypeError
        
        index = self._get_next_index()
        if index > MAX_UINT:
            raise ValueError(index)
        
        call.set_index(index)
        self._put_call(index, call)
        return index

    def print_calls(self):
        for call in self.get_calls():
            print(call._method_name, call._args, call._kwargs)

    def print_uncommitted_calls(self):
        for call in self.get_uncommitted_calls():
            print(call._method_name, call._args, call._kwargs)

    def get_calls(self) -> Generator[Call, None, None]:
        for _, call in self._db.scan(prefix=self.CALL_PREFIX):
            yield call

    def get_first_uncommitted_call(self) -> Call:
        next_offset = self._get_next_offset()
        if next_offset > self._get_index():
            return
        call_key = self._get_call_key(next_offset)
        return self._db.get(call_key)

    def get_uncommitted_calls(self) -> Generator[Call, None, None]:
        first_uncommitted_call = self.get_first_uncommitted_call()
        if first_uncommitted_call is not None:
            for index in range(first_uncommitted_call.index,
                               self._get_next_index()):
                yield self.get(index)

    def truncate(self):
        if self._max_committed_items is not None:
            committed_calls_no = self.count_committed_calls()
            for key, call in self._db.scan(prefix=self.CALL_PREFIX):
                if not call.committed:
                    break

                if committed_calls_no <= self._max_committed_items:
                    break

                self._db.delete(key)
                committed_calls_no -= 1

        if self._committed_ttl_seconds is not None:
            for key, call in self._db.scan(prefix=self.CALL_PREFIX):
                if not call.committed:
                    break

                min_timestamp = get_timestamp_ms() \
                    - self._committed_ttl_seconds * 1000
                if call._creation_timestamp <= min_timestamp:
                    self._db.delete(key)

    def count_committed_calls(self) -> int:
        counter = 0
        for _, call in self._db.scan(prefix=self.CALL_PREFIX):
            if call.committed:
                counter += 1
        return counter

    def count_calls(self) -> int:
        counter = 0
        for _, _ in self._db.scan(prefix=self.CALL_PREFIX):
            counter += 1
        return counter

    def _get_call_key(self, index: int):
        return self.CALL_PREFIX + utils.int_to_padded_bytes(index, UINT_BYTES)

    def _update_call(self, index: int, call: Call):
        if not isinstance(call, Call):
            raise TypeError
        call_key = self._get_call_key(index)
        self._db.put(call_key, call, write_batch=self._write_batch)

    def _put_call(self, index: int, call: Call):
        if not isinstance(call, Call):
            raise TypeError

        is_new_write_batch = self._write_batch is None
        if is_new_write_batch:
            self.begin()

        call_key = self._get_call_key(index)
        self._db.put(call_key, call, write_batch=self._write_batch)
        self._increment_index()

        if is_new_write_batch:
            self.commit()

    def _increment_offset(self):
        self._increment_int_attribute('offset')

    def _get_offset(self) -> int:
        return self._get_int_attribute('offset')

    def _get_next_offset(self) -> int:
        return self._get_int_attribute('offset') + 1

    def _increment_index(self):
        self._increment_int_attribute('index')

    def _get_index(self) -> int:
        if self._write_batch is not None:
            return self._batch_index
        return self._get_int_attribute('index')

    def _get_next_index(self) -> int:
        if self._write_batch is not None:
            return self._batch_index + 1
        return self._get_int_attribute('index') + 1

    def _get_meta_key(self, attribute: str):
        return self.META_PREFIX + bytes(attribute, 'utf-8')

    def _increment_int_attribute(self, attribute: str):
        if attribute == 'index' and self._write_batch is not None:
            self._batch_index += 1
            value = self._batch_index
        else:
            value = self._get_int_attribute(attribute) + 1

        key = self._get_meta_key(attribute)
        self._db.put(key, value, write_batch=self._write_batch)

    def _get_int_attribute(self, attribute: str) -> int:
        key = self._get_meta_key(attribute)
        value = self._db.get(key)
        if value is None:
            return -1
        return value
