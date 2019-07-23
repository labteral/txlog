#!/usr/bin/env python
# -*- coding: utf-8 -*-

from txlog import TxLog, Call
import shutil


def method_without_params():
    print('method_without_params executed')


def method_with_one_param(param1):
    print(f'method_with_one_param({param1}) executed')


def method_with_two_params(param1, param2=None):
    print(f'method_with_two_params({param1}, {param2}) executed')


print(globals()['method_with_two_params'])

TXLOG_PATH = './txlog'
shutil.rmtree(TXLOG_PATH)
txlog = TxLog(path=TXLOG_PATH)

call1 = Call('method_without_params')
call2 = Call('method_with_one_param', args=['value1'])
call3 = Call('method_with_two_params', kwargs={'param1': 'value1', 'param2': 'value2'})

txlog.begin()
txlog.add(call1)
txlog.add(call2)
txlog.add(call3)
txlog.commit()

txlog.print_uncommitted_calls()

txlog.exec_uncommitted_calls(globals())