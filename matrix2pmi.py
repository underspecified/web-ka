#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

'''
`matrix2pmi.py`: caches co-occurence frequencies and discounted PMI
between relation patterns and argument tuples into a matrix stored in
mongodb

### Usage

	Usage: matrix2pmi.py [options] [database] [collection]

	Options:
  	-h, --help            show this help message and exit
  	-o HOST, --host=HOST  mongodb host machine name
     			      default: localhost
  	-p PORT, --port=PORT  mongodb host machine port number
       			      default: 1979
  	-s START, --start=START
    	                      specify calculation to start with
        	              1 or F_i: instance tuple frequencies
            	              2 or F_p: relation pattern frequencies
                	      3 or F_ip: instance*pattern co-occurence frequencies
	                      4 or pmi_ip: instance*pattern discounted PMI score
                      	      default: F_i

### Caches Created

Creates 6 caches in the form of mongodb collections:

1. `<matrix>_F_all`: matrix with sum of scores for all (rel,args) tuples
2. `<matrix>_F_i`: argument instance frequencies
3. `<matrix>_F_p`: relation pattern frequencies
4. `<matrix>_F_ip`: instance*pattern co-occurence frequencies
5. `<matrix>_pmi_ip`: instance*pattern Pointwise Mutual Information
   score discounted to account for bias toward infrequent events
   following [1]
6. `<matrix>_max_pmi_ip`: caches the maximum dpmi value in <matrix>_pmi_ip

### Pointwise Mutual Information

Pointwise mutual information between argument instances and relation
patterns is defines following [2] as:

	(1) PMI(i,p) = log( F(i,p) / F(i)*F(p) )

where

	(2) F(i) = the frequency of argument instance i
	(3) F(p) = the frequency of relation pattern p
	(4) F(i,p) = the co-occurence frequency of argument instance i
	    and relation pattern p

### Discounted PMI

Pointwise Mutual Information is known to be biased toward infrequent
events. Pantel and Ravichandran [1] compensate by multiplying PMI by a
"discounting factor" that is essentially a smoothed co-occurence
frequency multiplied by a smoothed frequency of the argument instance
or the relation pattern, whichever is lesser.

	(5) discount(i,p) = (F(i,p) / F(i,p)+1) * (min(F(i),F(p)) / min(F(i),F(p))+1)
	(6) discountedPMI(i,p) = PMI(i,p) * discount(i,p)

### References

[1] Patrick Pantel and Deepak Ravichandran.
Automatically Labeling Semantic Classes.
HLT-NAACL 2004.

[2] Patrick Pantel and Marco Pennacchiotti.
Espresso: Leveraging Generic Patterns for Automatically Harvesting Semantic Relations.
ACL 2006.
'''

import pymongo
import sys
from bson.code import Code
from bson.son import SON
from collections import defaultdict
from functools import partial
from math import log

import mongodb
from instances2matrix import ensure_indices


class PMI:
    def __init__(self, db, matrix, batch=100):
        '''initializes class with information necessary for calculating PMI scores'''
        self.db = db
        self.matrix = matrix
        self.batch = batch
        self.argv = self.get_args()
        self.argc = len(self.argv)
        self._F_all = '%s_F_all' % self.matrix
        self._F_i = '%s_F_i' % self.matrix
        self._F_p = '%s_F_p' % self.matrix
        self._F_ip = '%s_F_ip' % self.matrix
        self._pmi_ip = '%s_pmi_ip' % self.matrix
        self._max_pmi_ip = '%s_max_pmi_ip' % self.matrix
        self.F_all = self.get_F_all()

    def get_args(self):
        '''returns a lists of argument names in <matrix>'''
        x = self.db[self.matrix].find_one()
        return sorted([k
                       for k in x.keys()
                       if k.startswith('arg')])

    def make_F_all(self):
        '''creates a collection <matrix>_F_all containing total frequency of 
        corpus and returns its name'''
        print >>sys.stderr, 'making all counts...'
        map_ = Code('function () {'
                    '  emit("all", {score:this.score});'
                    '}')
        reduce_ = Code('function (key, values) {'
                       '  var sum = 0;'
                       '  values.forEach('
                       '    function (doc) {sum += doc.score;}'
                       '  );'
                       '  return {score:sum};'
                       '}')
        r = self.db[self.matrix].map_reduce(
            map_, reduce_, self._F_all, full_response=True
            )
        print >>sys.stderr, 'making all counts: done.'

    def make_F_i(self):
        '''creates a collection <matrix>_F_i containing instance 
        frequencinces and returns its name'''
        print >>sys.stderr, 'making instance counts...'
        arg_str = ['%s:this.%s'%(a,a) 
                   for a in self.argv]
        map_ = Code('function () {'
                    '  var d = {};'
                    '  for (i=1;i<=n;i++) {d["arg"+i] = this["arg"+i]}'
                    '  emit(d, {score:this.score});'
                    '}', n=self.argc) # pass arg count as external argument
        reduce_ = Code('function (key, values) {'
                       '  var sum = 0;'
                       '  values.forEach('
                       '    function (doc) {sum += doc.score;}'
                       '  );'
                       '  return {score:sum};'
                       '}')
        self.db[self.matrix].map_reduce(
            map_, reduce_, self._F_i, full_response=True
            )
        print >>sys.stderr, 'making instance counts: done.'

    def make_F_p(self):
        '''creates a collection <matrix>_F_p containing relation pattern
        frequencinces and returns its name'''
        print >>sys.stderr, 'making pattern counts...'
        map_ = Code('function () {'
                    '  emit({rel:this.rel}, {score:this.score});'
                    '}')
        reduce_ = Code('function (key, values) {'
                       '  var sum = 0;'
                       '  values.forEach('
                       '    function (doc) {sum += doc.score;}'
                       '  );'
                       '  return {score:sum};'
                       '}')
        self.db[self.matrix].map_reduce(
            map_, reduce_, self._F_p, full_response=True
            )
        print >>sys.stderr, 'making pattern counts: done.'

    def make_F_ip(self):
        '''creates a collection <matrix>_F_ip containing instance*pattern
        frequencinces and returns its name'''
        print >>sys.stderr, 'making instance*pattern counts...'
        arg_str = ['%s:this.%s'%(a,a) 
                   for a in self.argv]
        map_ = Code('function () {'
                    '  var d = {};'
                    '  d["rel"] = this.rel;'
                    '  for (i=1;i<=n;i++) {d["arg"+i] = this["arg"+i]}'
                    '  emit(d, {score:this.score});'
                    '}', n=self.argc) # pass arg count as external argument
        reduce_ = Code('function (key, values) {'
                       '  var sum = 0;'
                       '  values.forEach('
                       '    function (doc) {sum += doc.score;}'
                       '  );'
                       '  return {score:sum};'
                       '}')
        self.db[self.matrix].map_reduce(
            map_, reduce_, self._F_ip, full_response=True
            )
        print >>sys.stderr, 'making instance*pattern counts: done.'

    def make_pmi_ip(self):
        '''creates a collection <matrix>_pmi_ip containing instance*relation
        Pointwise Mutual Information scores and returns its name'''
        print >>sys.stderr, 'calculating instance*pattern PMI...'
        xs = mongodb.fast_find(self.db, self.matrix, batch=self.batch)
        for n,x in enumerate(xs, 1):
            p = x['rel']
            rel = [('rel', p), ]
            i = [x[a] for a in self.argv]
            args = zip(self.argv, i)
            pmi = zip(('dpmi', 'discount', 'pmi'), self.discounted_pmi(i,p))
            y = SON(rel+args+pmi)
            self.db[self._pmi_ip].save(y)
            if n%10000 == 0:
                print >>sys.stderr, '# %8d PMI scores calculated' % n
        print >>sys.stderr, 'calculating instance*pattern PMI: done.'
        ensure_indices(self.db, self._pmi_ip)
        self.db[self._pmi_ip].ensure_index(
            [('dpmi', pymongo.DESCENDING), ]
            )

    def pmi(self, i, p):
        '''retrieves pmi value for (i,p) from matrix'''
        try:
            q = mongodb.make_query(i,p)
            #print >>sys.stderr, 'q:', q
            r = self.db[self._pmi_ip].find_one(q)
            #print >>sys.stderr, 'pmi:', q, r
            return r['dpmi']
        except Exception as e:
            return 0.0

    def make_max_pmi_ip(self):
        '''caches the maximum value for dpmi in <matrix>_pmi_ip to 
        <matrix>_max_pmi_ip'''
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
        r = self.db[self._pmi_ip].map_reduce(
            map_, reduce_, self._max_pmi_ip, full_response=True,
            )
        print >>sys.stderr, 'calculating max PMI: done.'

    def max_pmi(self):
        '''finds maximum pmi value in matrix'''
        r = self.db[self._max_pmi_ip].find_one()
        print >>sys.stderr, 'max_pmi():', r
        return r['value']

    def get_F_all(self):
        '''gets sum of scores for all (rel,args) tuples in <matrix>'''
        def r2score(r):
            score = r['value']['score']
            print >>sys.stderr, 'F_all:', score
            assert float(score)
            return score
        r = self.db[self._F_all].find_one()
        if not r:
            self.make_F_all()
            r = self.db[self._F_all].find_one()
        return r2score(r)

    def F_i(self, i):
        '''calculate the frequency (i.e. the sum of scores) of an argument 
        instance'''
        try:
            query = mongodb.make_query(i)
            #print >>sys.stderr, 'query:', query
            #print >>sys.stderr, self.db[self._F_i].find(query).explain()
            v = self.db[self._F_i].find_one(query, fields=['value'])
            #print >>sys.stderr, 'v:', v
            return v['value']['score']
        except Exception as e:
            print >>sys.stderr, e, i
            raise e

    def F_p(self, p):
        '''calculate the frequency (i.e. the sum of scores) of a relation 
        pattern'''
        try:
            query = mongodb.make_query(i=None,p=p)
            #print >>sys.stderr, 'query:', query
            #print >>sys.stderr, self.db[self._F_p].find(query).explain()
            v = self.db[self._F_p].find_one(query, fields=['value'])
            #print >>sys.stderr, 'v:', v
            return v['value']['score']
        except Exception as e:
            print >>sys.stderr, e, p
            raise e

    def F_ip(self, i, p):
        '''calculate the co-occurence frequency (i.e. the sum of scores) of 
        instance*pattern'''        
        try:
            query = mongodb.make_query(i,p=None)
            #print >>sys.stderr, 'query:', query
            #print >>sys.stderr, self.db[self._F_ip].find(query).explain()
            v = self.db[self._F_ip].find_one(query, fields=['value'])
            #print >>sys.stderr, 'v:', v
            return v['value']['score']
        except Exception as e:
            print >>sys.stderr, e, i, p
            raise e

    def calc_pmi(self, i, p):
        '''pmi: pointwise mutual information between instance and pattern'''
        return self._pmi(self.F_all, self.F_i(i), self.F_p(p), self.F_ip(i,p))

    def _calc_pmi(self, F_all, F_i, F_p, F_ip):
        '''pmi: pointwise mutual information between instance and pattern'''
        P_i = F_i / F_all
        P_p = F_p / F_all
        P_ip = F_ip / F_all
        return log( P_ip / (P_i*P_p) )

    def smooth(self, x, y=1.0):
        '''returns value of x smoothed by y'''
        return x / (x+y)

    def discount(self, i, p):
        '''discounting factor for PMI towards infrequent elements
        See Equation (2) from:
        Patrick Pantel and Deepak Ravichandran.
        Automatically Labeling Semantic Classes.
        HLT-NAACL 2004.'''
        return self._discount( 
            c_ef=self.F_ip(i,p), c_ei=self.F_i(i), c_jf=self.F_p(p) 
            )

    def _discount(self, c_ef, c_ei, c_jf):
        '''discounting factor for PMI towards infrequent elements
        See Equation (2) from:
        Patrick Pantel and Deepak Ravichandran.
        Automatically Labeling Semantic Classes.
        HLT-NAACL 2004.'''
        return self.smooth(c_ef,1.0) * self.smooth(min(c_ei,c_jf),1.0)

    def discounted_pmi(self, i, p):
        '''returns a tuple of (discount*pmi, discount, pmi)'''
        F_i = self.F_i(i)
        F_p = self.F_p(p)
        F_ip = self.F_ip(i,p) 
        pmi = self._calc_pmi(self.F_all, F_i, F_p, F_ip)
        discount = self._discount(c_ef=F_ip, c_ei=F_i, c_jf=F_p)
        dpmi = pmi*discount
        return dpmi, discount, pmi

def validate_start(s):
    '''maps starting collection name to its order of collection
    returning 0 if invalid'''
    fs = list(
        enumerate(('F_all', 'F_i', 'F_p', 'F_pi', 'pmi_ip', 'max_pmi_ip'), 1)
        )
    ns = list(enumerate(('1', '2', '3', '4', '5', '6'), 1))
    d = defaultdict(int, {k:v for v,k in fs+ns})
    return d[s]

def main():
    from optparse import OptionParser
    usage = '''%prog [options] [database] [collection]'''
    parser = OptionParser(usage=usage)
    parser.add_option('-o', '--host', dest='host', default='localhost',
                      help='''mongodb host machine name. default: localhost''')    
    parser.add_option('-p', '--port', dest='port', type=int, default=27017,
                      help='''mongodb host machine port number. default: 27017''')
    parser.add_option('-s', '--start', dest='start', default='F_i',
                      help='''specify calculation to start with
                              1 or F_all: sum of all scores for (rel,args) tuples
                              2 or F_i: instance tuple frequencies
                              3 or F_p: relation pattern frequencies
                              4 or F_ip: instance*pattern co-occurence frequencies
                              5 or pmi_ip: instance*pattern Pointwise Mutual Information score
                              6 or max_pmi_ip: maximum Pointwise Mutual Information score
                              default: F_i''')
    options, args = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        exit(1)
    start = validate_start(options.start)
    if start == 0:
        print >>sys.stderr, 'start option is invalid! %s' % option.start
        parser.print_help()
        exit(1)

    db, collection = args
    connection = pymongo.Connection(options.host, options.port)
    p = PMI(connection[db], collection)

    #if start <= 1:
    #    p.make_F_all()
    if start <= 2:
        p.make_F_i()
    if start <= 3:
        p.make_F_p()
    if start <= 4:
        p.make_F_ip()
    if start <= 5:
        p.make_pmi_ip()
    if start <= 6:
        p.make_max_pmi_ip()

if __name__ == '__main__':
    main()
