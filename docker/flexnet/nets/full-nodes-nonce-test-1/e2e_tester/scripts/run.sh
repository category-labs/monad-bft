#!/bin/bash

mkdir /monad/logs

python3 /monad/scripts/e2e.py  2> /monad/logs/e2e-tester.err 1> /monad/logs/e2e-tester.log