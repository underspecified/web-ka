#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

'''
`espresso.py`: an implemenatation of the Espresso bootstrapping algorithm

### Usage

        Usage: espresso.py [options] [database] [collection] [rel] [seeds]

        Options:
          -h, --help            show this help message and exit
          -o HOST, --host=HOST  mongodb host machine name. default: localhost
          -p PORT, --port=PORT  mongodb host machine port number. default: 27017
          -s START, --start=START
                                iteration to start with. default: 1
          -t STOP, --stop=STOP  iteration to stop at. default: 2

### Caches Created

Creates 2 caches of bootstrapped instances and patterns for the target 
relation:

1. `<matrix>_<rel>_esp_i`: bootstrapped instances for <rel>
2. `<matrix>_<rel>_esp_p`: bootstrapped patterns for <rel>

### Bootstrapping

Bootstrapping starts with seed instances and alternates between promoting new
patterns and instances following the Espresso bootstrapping algorithm [1].

1. retrieve promoted instances/patterns
2. rank by reliability score
3. keep top 10 promoted instances/patterns
4. bootstrap patterns/instances using promoted instances/patterns

### Reliability Score

Candidate patterns and instances are ranked by reliability score, which 
reflects the pointwise mutual information score between a promoted 
pattern/instance and the set of instances/patterns that generated it.

        (1) r_i(i,P) = sum( dpmi(i,p)*r_p(p) / max_pmi ) / len(P)
                         for p in P

        (2) r_p(P,i) = sum( dpmi(i,p)*r_i(i) / max_pmi ) / len(I)
                         for i in I

where dpmi is Discounted Pointwise Mutual Information [2].
r_i and r_p are recursively defined with r_i=1.0 for the seed instances.

### References

[1] Patrick Pantel and Marco Pennacchiotti.
Espresso: Leveraging Generic Patterns for Automatically Harvesting Semantic Relations.
ACL 2006.
'''

import fileinput
import pymongo
import sys
from bson.code import Code
from bson.son import SON

import mongodb


def has_run(db, coll, i=0):
    '''determines if db.coll has iteration it'''
    if db[coll].find_one({'it':i}):
        return True
    else:
        return False

def has_seeds(db, coll):
    '''determines if db.coll has seed iteration'''
    return has_run(db, coll, 0)

def add_seeds(db, coll, seeds):
    '''adds seeds to db.coll with reliability score of 1.0'''
    for s in seeds:
        args = s.split('\t')
        doc = {'arg%d'%n:v
               for n,v in enumerate(args, 1)}
        doc['it'] = 0
        doc['r_i'] = 1.0
        cache(db, coll, doc)

def cache(db, coll, doc):
    '''save doc to db.coll if it doesn't already exist'''
    if not db[coll].find_one(doc):
        db[coll].save(doc)

def i2query(i):
    '''returns a tuple of argument values labeled (arg1,<value>),
    (arg2,<value>), ..., (argn,<value>) '''
    return zip(['arg%d'%n for n in xrange(1,len(i)+1)], i)

def make_query(i=None, p=None):
    '''generates a mongodb query from i and p, placing p before i to match 
    index order'''
    #q = SON()
    q = {}
    if p:
        q['rel'] = p
    if i:
        for k,v in i2query(i):
            q[k] = v
    #print >>sys.stderr, 'make_query:', i, p, q
    return q

def max_pmi(db, matrix):
    '''finds maximum pmi value in matrix'''
    max_pmi_ip = '%s_max_pmi_ip' % matrix
    r = db[max_pmi_ip].find_one()
    return r['results'][0]['value']

def pmi(db, matrix, i, p):
    '''retrieves pmi value for (i,p) from matrix'''
    try:
        _pmi_ip = '%s_pmi_ip' % matrix
        #r = db[_pmi_ip].find_one(make_query(i,p), fields=['dpmi'])
        q = make_query(i,p)
        #print >>sys.stderr, 'q:', q
        r = db[_pmi_ip].find_one(q)
        #print >>sys.stderr, 'pmi:', q, r
        return r['dpmi']
    except Exception as e:
        return 0.0

def _r_i(db, matrix, i):
    '''retrieves r_i for past iteration'''
    try:
        _esp_i = '%s_esp_i' % matrix
        r = db[_esp_i].find_one(make_query(i=i,p=None), fields=['r_i'])
        #print >>sys.stderr, 'r_i:', r
        return r['r_i']
    except Exception as e:
        return 0.0
        
def _r_p(db, matrix, p):
    '''retrieves r_p for past iteration'''
    try:
        esp_p = '%s_esp_p' % matrix
        r = db[esp_p].find_one(make_query(i=None,p=p), fields=['r_p'])
        #print >>sys.stderr, 'r_p:', r
        return r.get('r_p',0.0)
    except Exception as e:
        return 0.0
    
def r_p(db, matrix, I, p, _max_pmi):
    '''r_p: reliability of pattern p'''
    print >>sys.stderr, 'r_p(%s, %s, %s):' % (p, I, _max_pmi) 
    r = sum( [pmi(db,matrix,i,p)*_r_i(db,matrix,i)/_max_pmi
              for i in I] ) / len(I)
    print >>sys.stderr, 'r_p:', p, r
    return r

def r_i(db, matrix, i, P, _max_pmi):
    '''r_i: reliability of instance i'''
    r = sum( [pmi(db,matrix,i,p)*_r_p(db,matrix,p)/_max_pmi
              for p in P] ) / len(P)
    print >>sys.stderr, 'r_i:', i, r
    return r

def S(i, P):
    '''confidence in an instance'''
    T = sum ( [ _r_p(db,matrix,p) for p in P ] )
    sum ( [ pmi(db,matrix,i,p) * _r_p(db,matrix,p) / T
            for p in P ] )

def get_args(db, matrix):
    '''returns a lists of argument names in <matrix>'''
    x = db[matrix].find_one()
    return sorted([k 
                   for k in x.keys()
                   if k.startswith('arg')])

def get_I(db, matrix, it, query={}):
    '''retrieves instances that match query from iteration it'''
    query['it'] = it
    esp_i = '%s_esp_i' % matrix
    args = get_args(db, matrix)
    return [[v
             for k,v in sorted(r.items()) 
             if k.startswith('arg')]
             for r in mongodb.fast_find(db, esp_i, query, fields=args) ]

def get_P(db, matrix, it, query={}):
    '''retrieves patterns that match query from iteration it'''
    query['it'] = it
    esp_p = '%s_esp_p' % matrix
    return [r['rel'] 
            for r in mongodb.fast_find(db, esp_p, query, fields=['rel']) ]

def I2P(db, matrix, I):
    '''retrieve patterns that match promoted instances in I and have not been
    retrieved in past iteration'''
    esp_p = '%s_esp_p' % matrix
    P = [r['rel']
         for i in I
         for r in mongodb.fast_find(
            db, matrix, make_query(i=i,p=None), fields=['rel']
            )
         if not db[esp_p].find_one({'rel':r['rel']}) ]
    P_ = sorted(set(P))
    print >>sys.stderr, 'P: %d => %d' % (len(P), len(P_))
    return P_

def P2I(db, matrix, P):
    '''retrieve instances that match promoted patterns in P and have not been
    retrieved in past iteration'''
    esp_i = '%s_esp_i' % matrix
    args = get_args(db, matrix)
    I = [tuple( [v
                 for k,v in sorted(r.items())
                 if k.startswith('arg')] )
         for p in P
         for r in mongodb.fast_find(
            db, matrix, make_query(i=None,p=p), fields=args
            )
         if not db[esp_i].find_one(
            {k:v 
             for k,v in sorted(r.items())
             if k.startswith('arg')} ) ]
    I_ = sorted(set(I))
    print >>sys.stderr, 'I: %d => %d' % (len(I), len(I_))
    return I_

def rank_patterns(db, matrix, I, P, it, _max_pmi):
    '''return a list of patterns ranked by reliability score'''
    rs = [{'rel':p, 'it':it, 'r_p':r_p(db,matrix,I,p,_max_pmi)} 
          for p in P]
    rs.sort(key=lambda r: r.get('r_p',0.0),reverse=True)
    return rs

def rank_instances(db, matrix, I, P, it, _max_pmi):
    '''return a list of instances ranked by reliability score'''
    rs = []
    for i in I:
        r = {'arg%d'%n:v
             for n,v in enumerate(i, 1)}
        r['it'] = it
        r['r_i'] = r_i(db,matrix,i,P,_max_pmi)
        rs.append(r)
    rs.sort(key=lambda r: r.get('r_i',0.0),reverse=True)
    return rs

def bootstrap_p(db, matrix, rel, it, _max_pmi, n=10):
    '''perform an iteration of bootstrapping saving n patterns with the 
    highest reliability score'''
    # read promoted instances of last bootstrpping iteration
    print >>sys.stderr, 'getting promoted instances...'''
    I = get_I(db, matrix, it-1)
    print >>sys.stderr, 'I:', len(I)
    print >>sys.stderr, 'getting promoted instances: done.'''

    # find matching patterns
    print >>sys.stderr, 'getting matching patterns...'
    P = I2P(db, matrix, I)
    print >>sys.stderr, 'getting matching patterns: done.'

    # rank patterns by reliability score
    print >>sys.stderr, 'ranking patterns by reliability score...'
    rs = rank_patterns(db, matrix, I, P, it, _max_pmi)
    print >>sys.stderr, 'ranking patterns by reliability score: done.'

    # save top n to <matrix>_esp_p
    print >>sys.stderr, 'saving top %d patterns...' % n
    esp_p = '%s_%s_esp_p' % (matrix, rel)
    for r in rs[:n]:
        print >>sys.stderr, 'r:', r
        cache(db, esp_p, r)
    print >>sys.stderr, 'saving top %d patterns: done.' % n

    print >>sys.stderr, 'ensuring indices ...'
    # index for iteration number
    db[esp_p].ensure_index( [('it', pymongo.DESCENDING), ] )
    # index for <REL>
    db[esp_p].ensure_index( [('rel', pymongo.ASCENDING), ] )
    print >>sys.stderr, 'ensuring indices: done.'

def bootstrap_i(db, matrix, rel, it, _max_pmi, n=10):
    '''perform an iteration of bootstrapping saving n instances with the 
    highest reliability score'''
    # read promoted patterns of last bootstrpping iteration
    print >>sys.stderr, 'getting promoted patterns...'''
    P = get_P(db, matrix, it)
    print >>sys.stderr, 'P:', len(P)
    print >>sys.stderr, 'getting promoted patterns: done.'''

    # find matching instances
    print >>sys.stderr, 'getting matching instances...'
    I = P2I(db, matrix, P)
    print >>sys.stderr, 'getting matching instances: done.'

    # rank instances by reliability score
    print >>sys.stderr, 'ranking instances by reliability score...'
    rs = rank_instances(db, matrix, I, P, it, _max_pmi)
    print >>sys.stderr, 'ranking instances by reliability score: done.'

    # save top n to <matrix>_esp_p
    print >>sys.stderr, 'saving top %d instances...' % n
    esp_i = '%s_%s_esp_i' % (matrix, rel)
    for r in rs[:n]:
        print >>sys.stderr, 'r:', r
        cache(db, esp_i, r)
    print >>sys.stderr, 'saving top %d instances: done.' % n

    print >>sys.stderr, 'ensuring indices ...'
    # index for iteration number
    db[esp_i].ensure_index( [('it', pymongo.DESCENDING), ] )
    # index for <ARGJ,...,ARGN>
    db[esp_i].ensure_index(
        [(arg, pymongo.ASCENDING)
        for arg in get_args(db, esp_i)]
    )
    print >>sys.stderr, 'ensuring indices: done.'

def espresso(db, matrix, rel, start, stop):
    '''apply espresso bootstrapping algorithm for rel from iteration start to
    stop'''
    _max_pmi = max_pmi(db, matrix)
    for it in xrange(start, stop):
        print >>sys.stderr, 'pattern bootstrapping iter: %d' % it
        bootstrap_p(db, matrix, rel, it, _max_pmi)
        print >>sys.stderr, 'instance bootstrapping iter: %d' % it
        bootstrap_i(db, matrix, rel, it, _max_pmi)

def main():
    from optparse import OptionParser
    usage = '''%prog [options] [database] [collection]'''
    parser = OptionParser(usage=usage)
    parser.add_option('-o', '--host', dest='host', default='localhost',
                      help='''mongodb host machine name. default: localhost''')    
    parser.add_option('-p', '--port', dest='port', type=int, default=27017,
                      help='''mongodb host machine port number. default: 27017''')
    parser.add_option('-s', '--start', dest='start', type=int, default=1,
                      help='''iteration to start with. default: 1''')
    parser.add_option('-t', '--stop', dest='stop', type=int, default=2,
                      help='''iteration to stop at. default: 2''')
    options, args = parser.parse_args()
    if len(args) != 3:
        parser.print_help()
        exit(1)
    db_, matrix, rel = args[:3]
    files = args[4:]
    connection = pymongo.Connection(options.host, options.port)
    db = connection[db_]
    seeds = (i.strip() for i in fileinput.input(files))
    esp_i = '%s_%s_esp_i' % (matrix, rel)
    if not has_seeds(db, esp_i):
        add_seeds(db, esp_i, seeds)
    espresso(db, matrix, rel, options.start, options.stop)

if __name__ == '__main__':
    main()
