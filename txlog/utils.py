#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time


def get_timestamp_ms():
    return int(round(time.time() * 1000))
