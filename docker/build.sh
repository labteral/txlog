#!/bin/bash
tar --dereference -cf txlog.tar \
    ../txlog \
    ../setup.py
docker build -t txlog -f Dockerfile .
rm txlog.tar
