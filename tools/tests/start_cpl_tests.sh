#!/bin/bash

mkdir -p logs

for k in nokeep keep; do
    for s in pc rel; do
	(python cpl.py ini/cpl.$s.$k.ini --start 1 --stop 50 2>&1 | tee logs/cpl.$s.$k.log)&
    done
done
