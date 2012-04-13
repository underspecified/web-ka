#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

'''
instances2matrix.py: creates a matrix of co-occurence counts between relation 
pattern x arguments in mongodb from input instances

## Instance Format

Instances have the following tab-delimited format:

* score: score representing weight * co-occurence count for instance
* loc: giving source and location of instance
* rel: containing relation pattern
* argc: giving argument count
* argv: tab-delimited list of arguments as strings

Example:

     1.0\treverb_clueweb_tuples-1.1.txt:30:10-11\tARG1 acquired ARG2\t2\Google\tYouTube

## Matrix Database Format

The co-occurence matrix has the following fields:

* rel: relation pattern
* arg1: first argument
* ...
* argn: nth argument

It is indexed for fast look up of rel, args, and (rel,args) tuples.
'''

import fileinput
import functools
import pymongo
import sys
from collections import namedtuple


Instance = namedtuple('Instance', ['score', 'loc', 'rel', 'argc', 'argv'])

def str2instance(s):
    '''converts tab-delimited string into Instance'''
    ss = s.strip().split('\t')
    score, loc, rel, argc = ss[:4]
    argv = ss[4:]
    score = float(score)
    argc = int(argc)
    assert len(argv) == argc
    return Instance(score, loc, rel, argc, argv)

def instance2doc(i):
    '''converts Instance into mongodb document (i.e. dictionary), enumerating all 
    args in argv'''
    doc = {'arg%d'%n:v
         for n,v in enumerate(i.argv, 1)}
    doc['score'] = i.score
    doc['rel'] = i.rel
    return doc

def ensure_indices(collection, argc):
    '''ensures indices exist on collection for <REL,ARG1,...ARGN> and 
    <ARG1,...,ARGN>, <ARG2,...,ARGN>, ..., <ARGN>'''
    # index for <REL,ARG1,...ARGN>
    collection.ensure_index(
        [('rel', pymongo.ASCENDING), ] + \
            [('arg%d'%i, pymongo.ASCENDING)
             for i in xrange(1, argc+1)]
        )
    for i in xrange(1, argc+1):
        # index for <ARGJ,...,ARGN>
        collection.ensure_index(
            [('arg%d'%j, pymongo.ASCENDING)
             for j in xrange(i, argc+1)]
            )

def create_collection(collection, argc, data):
    '''creates collection containing instances from input files'''
    # ensure indices exist
    ensure_indices(collection, argc)
    for a in data:
        i = str2instance(a)
        print >>sys.stderr, i
        d = instance2doc(i)
        collection.save(d)


if __name__ == '__main__':
    from optparse import OptionParser
    usage = '''%prog [options] [<instance_file>]'''
    parser = OptionParser(usage=usage)
    parser.add_option('-a', '--argc', dest='argc', type=int,
                      help='''number of arguments per instance''')
    parser.add_option('-c', '--collection', dest='collection',
                      help='''collection name''')
    parser.add_option('-d', '--database', dest='db', help='''database name''')
    parser.add_option('-o', '--host', dest='host', default='localhost',
                      help='''mongodb host machine name. default: localhost''')    
    parser.add_option('-p', '--port', dest='port', type=int, default=1979,
                      help='''mongodb host machine port number. default: 1979''')
    options, args = parser.parse_args()
    if options.db == None or options.collection == None or options.argc == None:
        parser.print_help()
        exit(1)
    connection = pymongo.Connection(options.host, options.port)
    collection = connection[options.db][options.collection]
    data = (i.strip() for i in fileinput.input(args))
    create_collection(collection, options.argc, data)
