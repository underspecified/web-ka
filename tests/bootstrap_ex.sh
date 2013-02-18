#!/bin/bash

# start mongod on port 1979 with database in test.data/mongo/dbs
mongod --config test.data/mongo/mongodb.conf&

# database instances in test.data/test_1000.txt
python tools/instances2matrix.py --port=1979 --database=test --collection=test_1000 test.data/test_1000.txt


python tools/matrix2pmi.py --port 1979 --start F_all test test_1000_2
