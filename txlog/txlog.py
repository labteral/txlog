#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .utils import get_timestamp_ms
from easyrocks import RocksDB, WriteBatch, utils
import builtins
from threading import RLock
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
        self._batch_offset = None
        self._write_batch = None
        self._write_lock = RLock()
        self._execution_lock = RLock()
        self._committed_ttl_seconds = committed_ttl_seconds
        self._max_committed_items = max_committed_items
        self._db = RocksDB(f'{path}')

    def begin(self):
        """Own the batch in this thread until commit or rollback."""
        self._write_lock.acquire()
        if self._write_batch is not None:
            self._write_lock.release()
            return
        try:
            self._batch_index = self._get_int_attribute('index')
            self._batch_offset = self._get_int_attribute('offset')
            self._write_batch = WriteBatch()
        except BaseException:
            self._write_lock.release()
            raise

    def commit(self):
        with self._write_lock:
            if self._write_batch is None:
                raise ValueError('No active transaction')
            try:
                self._db.commit(self._write_batch)
            finally:
                self._write_batch = None
                self._batch_index = self._batch_offset = None
                self._write_lock.release()

    def rollback(self):
        with self._write_lock:
            if self._write_batch is not None:
                self._write_batch = None
                self._batch_index = self._batch_offset = None
                self._write_lock.release()

    def commit_call(self, call: Call):
        with self._write_lock:
            if not isinstance(call, Call) or type(call.index) is not int:
                raise TypeError('Expected a persisted Call')
            expected = self._get_next_offset()
            if call.index < expected:
                return
            if call.index != expected or self.get(call.index) is None:
                raise ValueError('Calls must be committed in order')
            is_new_write_batch = self._write_batch is None
            previous = call._committed, call._commitment_timestamp
            try:
                if is_new_write_batch:
                    self.begin()
                call._committed = True
                call._commitment_timestamp = get_timestamp_ms()
                self._update_call(call.index, call)
                self._increment_offset()
                if is_new_write_batch:
                    self.commit()
            except BaseException:
                call._committed, call._commitment_timestamp = previous
                if is_new_write_batch:
                    self.rollback()
                raise

    def get(self, index: int) -> Call:
        with self._write_lock:
            return self._db.get(self._get_call_key(index))

    def exec_uncommitted_calls(self, container_object=None):
        with self._write_lock:
            if self._write_batch is not None:
                raise RuntimeError('Commit or roll back before replaying calls')
        with self._execution_lock:
            for call in self.get_uncommitted_calls():
                call.exec(container_object)
                self.commit_call(call)

    def add(self, call: Call):
        if not isinstance(call, Call):
            raise TypeError

        with self._write_lock:
            index = self._get_next_index()
            if index > MAX_UINT:
                raise ValueError(index)
            implicit = self._write_batch is None
            try:
                call.set_index(index)
                self._put_call(index, call)
                return index
            except BaseException:
                if implicit:
                    self.rollback()
                raise

    def print_calls(self):
        for call in self.get_calls():
            print(call._method_name, call._args, call._kwargs)

    def print_uncommitted_calls(self):
        for call in self.get_uncommitted_calls():
            print(call._method_name, call._args, call._kwargs)

    def get_calls(self) -> Generator[Call, None, None]:
        with self._write_lock:
            iterator = self._db.db.iteritems()
            iterator.seek(self.CALL_PREFIX)
        for key, value in iterator:
            if not key.startswith(self.CALL_PREFIX):
                break
            yield utils.unpack(value)

    def get_first_uncommitted_call(self) -> Call:
        with self._write_lock:
            next_offset = self._get_int_attribute('offset') + 1
            if next_offset > self._get_int_attribute('index'):
                return
            call = self.get(next_offset)
            if call is None:
                raise ValueError(f'Missing uncommitted call at index {next_offset}')
            return call

    def get_uncommitted_calls(self) -> Generator[Call, None, None]:
        with self._write_lock:
            first = self._get_int_attribute('offset') + 1
            end = self._get_int_attribute('index') + 1
        for index in range(first, end):
            with self._write_lock:
                if index < self._get_int_attribute('offset') + 1:
                    continue
                call = self.get(index)
                if call is None:
                    raise ValueError(f'Missing uncommitted call at index {index}')
            yield call

    def truncate(self):
        with self._write_lock:
            self._truncate()

    def _truncate(self):
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

    def _get_next_offset(self) -> int:
        if self._write_batch is not None:
            return self._batch_offset + 1
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
        elif attribute == 'offset' and self._write_batch is not None:
            self._batch_offset += 1
            value = self._batch_offset
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
