#!/usr/bin/env bash

python -m pip install --upgrade pip

python -m pip install -r requirements.txt \
    -r web/requirements.txt \
    -r bobvault/requirements.txt \
    -r balances/requirements.txt