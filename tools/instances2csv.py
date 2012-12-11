#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

'''
instances2csv.py: extract knowledge acquisition instance results from a
mongodb collection to a comma-seperated value spreadsheet
'''

import csv
import pymongo
import sys

import mongodb

def main():
    from optparse import OptionParser
    usage = '''%prog [options] [database] [collection]'''
    parser = OptionParser(usage=usage)
    parser.add_option('-o', '--host', dest='host', default='localhost',
                      help='''mongodb host machine name. default: localhost''')    
    parser.add_option('-p', '--port', dest='port', type=int, default=27017,
                      help='''mongodb host machine port number. default: 27017''')
    options, args = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        exit(1)
    db_, coll = args
    connection = pymongo.Connection(options.host, options.port)
    db = connection[db_]
    esp_i_writer = csv.DictWriter(
        sys.stdout, 
        ('it', 'score', 'arg1', 'arg2', 'arg3'), 
        extrasaction='ignore')
    for r in mongodb.fast_find(db, coll):
        esp_i_writer.writerow(r)

if __name__ == '__main__':
    main()
