#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

'''
`instances2matrix.py`: creates a matrix of co-occurence counts between
relation pattern * arguments in mongodb from input instances

### Usage

	Usage: instances2matrix.py [options] [<instance_files>]

	Options:
  	-h, --help            show this help message and exit
  	-c COLLECTION, --collection=COLLECTION  
  		              collection name
  	-d DB, --database=DB  database name
  	-o HOST, --host=HOST  mongodb host machine name. 
  			      default: localhost
  	-p PORT, --port=PORT  mongodb host machine port number. 
  			      default: 27017

### Instances

#### Format

Instances have the following tab-delimited format:

* `score`: score representing weight * co-occurence count for instance
* `loc`: giving source and location of instance
* `rel`: containing relation pattern
* `argc`: giving argument count
* `argv`: tab-delimited list of arguments as strings

#### Example

    1.0\treverb_clueweb_tuples-1.1.txt:30:10-11\tARG1 acquired ARG2\t2\Google\tYouTube
     
### Co-occurence Matrix

#### Format
   
The co-occurence matrix collection has the following fields:
     
* `rel`: relation pattern
* `arg1`: first argument
* ...
* `argn`: nth argument
* `score`: score for rel * args tuple

#### Naming Scheme

Instances of differing argument count are stored in separate mongodb
collections with names formatted as `<collection>_<argc>`. E.g. if a
collection `clueweb` has instances with argument counts of 1, 2, and
3, then the following collection would be created:
 
* `clueweb_1`
* `clueweb_2`
* `clueweb_3`
 
#### Indexing

It is indexed for fast look up of rel, args, and (rel,args) tuples.
'''

import fileinput
import functools
import pymongo
import re
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

def collection2argc(c):
    '''splits collection name into baseform and argc'''
    return int(c.split('_')[-1])

def is_matrix_collection(matrix, collection):
    '''returns true if collection name has the form <matrix>_<digit>'''
    return re.match('^%s_[0-9]+$' % matrix, collection)

def ensure_indices(db, coll):
    x = db[coll].find_one()
    n = len( [k 
              for k in x.keys()
              if k.startswith('arg')] )
    # index for <REL,ARG1,...ARGN>
    db[coll].ensure_index(
        [('rel', pymongo.ASCENDING), ] + \
            [('arg%d'%i, pymongo.ASCENDING)
             for i in xrange(1, n+1)]
        )
    for i in xrange(1, n+1):
        # index for <ARGJ,...,ARGN>
        db[coll].ensure_index(
            [('arg%d'%j, pymongo.ASCENDING)
             for j in xrange(i, n+1)]
            )

def ensure_matrix_indices(db, matrix):
    '''ensures indices exist on collection for <REL,ARG1,...ARGN> and 
    <ARG1,...,ARGN>, <ARG2,...,ARGN>, ..., <ARGN>'''
    print >>sys.stderr, 'ensuring indices for %s ...' % matrix
    for c in (c
              for c in db.collection_names()
              if is_matrix_collection(collection, c)):
        ensure_indices(db, c)
    print >>sys.stderr, 'ensuring indices for %s: done.' % collection

def collection_argc(c, argc):
    '''returns collection name appended with _argc'''
    return '%s_%d' % (c, argc)

def create_collection(db, collection, data):
    '''creates collection containing instances from input files'''
    for a in data:
        i = str2instance(a)
        print >>sys.stderr, i
        d = instance2doc(i)
        c = collection_argc(collection, i.argc)
        db[c].save(d)
    # ensure indices exist
    ensure_indices(db, collection)


if __name__ == '__main__':
    from optparse import OptionParser
    usage = '''%prog [options] [<instance_file>]'''
    parser = OptionParser(usage=usage)
    parser.add_option('-c', '--collection', dest='collection',
                      help='''collection name''')
    parser.add_option('-d', '--database', dest='db', help='''database name''')
    parser.add_option('-o', '--host', dest='host', default='localhost',
                      help='''mongodb host machine name. default: localhost''')    
    parser.add_option('-p', '--port', dest='port', type=int, default=1979,
                      help='''mongodb host machine port number. default: 27017''')
    options, args = parser.parse_args()
    if options.db == None or options.collection == None:
        parser.print_help()
        exit(1)
    connection = pymongo.Connection(options.host, options.port)
    db = connection[options.db]
    data = (i.strip() for i in fileinput.input(args))
    create_collection(db, options.collection, data)
