#!/usr/bin/env python
# -*- coding: utf-8 -*-

from txlog import TxLog, Call
import shutil
import os
import time

global_var = 0


def method_without_params():
    global global_var
    global_var += 1


def method_with_one_param(param1):
    global global_var
    global_var += 2


def method_with_two_params(param1, param2=None):
    global global_var
    global_var += 3


TXLOG_PATH = './txlog'
try:
    shutil.rmtree(TXLOG_PATH)
except FileNotFoundError:
    os.makedirs(TXLOG_PATH)
txlog = TxLog(path=TXLOG_PATH, max_committed_items=3, committed_ttl_seconds=1)

# Define three calls
call1 = Call('method_without_params')
call2 = Call('method_with_one_param', args=['value1'])
call3 = Call('method_with_two_params', kwargs={'param1': 'value1', 'param2': 'value2'})

# Add the three calls to the txlog atomically and check their indexes
txlog.begin()
assert txlog.add(call1) == 0
assert txlog.add(call2) == 1
assert txlog.add(call3) == 2
txlog.commit()

# Assert not one call is committed
for call in txlog.get_calls():
    assert not call.committed

# Assert there are three uncommitted calls registered
for i, call in enumerate(txlog.get_uncommitted_calls()):
    assert not call.committed
assert i == 2

# Execute the first call and check it is commited
txlog.get(0).exec(globals())
txlog.commit_call(0)
assert txlog.get(0).committed
assert txlog.count_committed_calls() == 1

# Check that the first method was executed correctly
assert global_var == 1

# Assert there are two calls to be committed yet
for i, call in enumerate(txlog.get_uncommitted_calls()):
    assert not call.committed
assert i == 1

# Exec the calls 2 and 3 and check that they were correctly executed
txlog.exec_uncommitted_calls(globals())
assert global_var == 6
assert txlog.count_calls() == 3
assert txlog.count_committed_calls() == 3

# Check there are not uncommitted calls left
for call in txlog.get_uncommitted_calls():
    assert False

# Check there are 3 calls in total already committed
for i, call in enumerate(txlog.get_calls()):
    assert call.committed
assert i == 2

# Check max_committed_items
assert txlog.add(call1) == 3
assert txlog.add(call2) == 4
assert txlog.add(call3) == 5
assert txlog.count_calls() == 6
assert txlog.count_committed_calls() == 3
txlog.exec_uncommitted_calls(globals())
assert txlog.count_calls() == 3
assert txlog.count_committed_calls() == 3

# Check committed_ttl_seconds
time.sleep(1)
txlog.truncate()
assert txlog.count_calls() == 0
assert txlog.count_committed_calls() == 0

print('PASS!')
