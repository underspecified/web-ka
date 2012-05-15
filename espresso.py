#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

import fileinput
import pymongo
import sys
from bson.code import Code
from bson.son import SON

import mongodb

'''
`espresso.py`: an implemenatation of the Espresso bootstrapping algorithm [1]

### Caches Created

2. `<matrix>_esp_i`
1. `<matrix>_esp_p`

'''

def has_run(db, coll, i=0):
    if db[coll].find_one({'it':i}):
        return True
    else:
        return False

def has_seeds(db, coll):
    return has_run(db, coll, 0)

def add_seeds(db, coll, seeds):
    for s in seeds:
        args = s.split('\t')
        doc = {'arg%d'%n:v
               for n,v in enumerate(args, 1)}
        doc['it'] = 0
        doc['r_i'] = 1.0
        cache(db, coll, doc)

def cache(db, coll, doc):
    if not db[coll].find_one(doc):
        db[coll].save(doc)

def i2query(i):
    '''returns a tuple of argument values labeled (arg1,<value>),
    (arg2,<value>), ..., (argn,<value>) '''
    return zip(['arg%d'%n for n in xrange(1,len(i)+1)], i)

def make_query(i=None, p=None):
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
    _pmi_ip = '%s_pmi_ip' % matrix
    print >>sys.stderr, 'calculating max PMI...'
    map_ = Code('function () {'
                '  emit("max", {dpmi:this.dpmi});'
                '}')
    reduce_ = Code('function (key, values) {'
                   '  var max = 0.0;'
                   '  values.forEach('
                   '    function (doc) {'
                   '      if ( doc.dpmi > max )'
                   '        max = doc.dpmi;'
                   '    }'
                   '  );'
                   '  return max;'
                   '}')
    r = db[_pmi_ip].map_reduce(
        map_, reduce_, {'inline':1},
        )
    print >>sys.stderr, 'calculating max PMI: done.'
    #print >>sys.stderr, r['results'][0]['value']
    return r['results'][0]['value']

def pmi(db, matrix, i, p):
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
    try:
        _esp_i = '%s_esp_i' % matrix
        r = db[_esp_i].find_one(make_query(i=i,p=None), fields=['r_i'])
        #print >>sys.stderr, 'r_i:', r
        return r['r_i']
    except Exception as e:
        return 0.0
    
    
def _r_p(db, matrix, p):
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
    query['it'] = it
    esp_i = '%s_esp_i' % matrix
    args = get_args(db, matrix)
    return [[v
             for k,v in sorted(r.items()) 
             if k.startswith('arg')]
             for r in mongodb.fast_find(db, esp_i, query, fields=args) ]

def get_P(db, matrix, it, query={}):
    query['it'] = it
    esp_p = '%s_esp_p' % matrix
    return [r['rel'] 
            for r in mongodb.fast_find(db, esp_p, query, fields=['rel']) ]

def I2P(db, matrix, I):
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
    rs = [{'rel':p, 'it':it, 'r_p':r_p(db,matrix,I,p,_max_pmi)} 
          for p in P]
    rs.sort(key=lambda r: r.get('r_p',0.0),reverse=True)
    return rs

def rank_instances(db, matrix, I, P, it, _max_pmi):
    rs = []
    for i in I:
        r = {'arg%d'%n:v
             for n,v in enumerate(i, 1)}
        r['it'] = it
        r['r_i'] = r_i(db,matrix,i,P,_max_pmi)
        rs.append(r)
    rs.sort(key=lambda r: r.get('r_i',0.0),reverse=True)
    return rs

def bootstrap_p(db, matrix, it, _max_pmi, n=10):
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
    esp_p = '%s_esp_p' % matrix
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

def bootstrap_i(db, matrix, it, _max_pmi, n=10):
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
    esp_i = '%s_esp_i' % matrix
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

def espresso(db, matrix, start, stop):
    _max_pmi = max_pmi(db, matrix)
    for it in xrange(start, stop):
        print >>sys.stderr, 'pattern bootstrapping iter: %d' % it
        bootstrap_p(db, matrix, it, _max_pmi)
        print >>sys.stderr, 'instance bootstrapping iter: %d' % it
        bootstrap_i(db, matrix, it, _max_pmi)

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
    if len(args) != 2:
        parser.print_help()
        exit(1)
    db, collection = args[:2]
    files = args[3:]
    connection = pymongo.Connection(options.host, options.port)
    seeds = (i.strip() for i in fileinput.input(files))
    esp_i = '%s_esp_i' % collection
    if not has_seeds(connection[db], esp_i):
        add_seeds(connection[db], esp_i, seeds)
    espresso(connection[db], collection, options.start, options.stop)

if __name__ == '__main__':
    main()
