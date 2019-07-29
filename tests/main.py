#!/usr/bin/env python
# -*- coding: utf-8 -*-

from txlog import TxLog, Call
import shutil
import os
global_var = 0


def method_without_params():
    print('method_without_params executed')
    global global_var
    global_var += 1


def method_with_one_param(param1):
    print(f'method_with_one_param({param1}) executed')
    global global_var
    global_var += 2


def method_with_two_params(param1, param2=None):
    print(f'method_with_two_params({param1}, {param2}) executed')
    global global_var
    global_var += 3


TXLOG_PATH = './txlog'
try:
    shutil.rmtree(TXLOG_PATH)
except FileNotFoundError:
    os.makedirs(TXLOG_PATH)
txlog = TxLog(path=TXLOG_PATH)

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

# Check that the first method was executed correctly
assert global_var == 1

# Assert there are two calls to be committed yet
for i, call in enumerate(txlog.get_uncommitted_calls()):
    assert not call.committed
assert i == 1

# Exec the calls 2 and 3 and check that they were correctly executed
txlog.exec_uncommitted_calls(globals())
assert global_var == 6

# Check there are not uncommitted calls left
for call in txlog.get_uncommitted_calls():
    assert False

# Check there are 3 calls in total already committed
for i, call in enumerate(txlog.get_calls()):
    assert call.committed
assert i == 2

print('\nPASS!')