#!/usr/bin/python
# -*- coding: utf-8 -*-
################################################################################

'''
matrix2pmi.py: calculates PMI between relation patterns and argument tuples in 
a co-occurence matrix

Creates 4 mongodb collections:


1. <matrix>_F_i: instance tuple frequencies
2. <matrix>_F_p: relation pattern frequencies
3. <matrix>_F_ip: instance*pattern co-occurence frequencies
4. <matrix>_pmi_ip: instance*pattern Pointwise Mutual Information score

'''

import pymongo
import sys
from bson.code import Code
from bson.son import SON
from math import log

from instances2matrix import ensure_indices

class PMI:
    def __init__(self, db, matrix):
        '''initializes class with information necessary for calculating PMI scores'''
        self.db = db
        self.matrix = matrix
        self.argv = self.get_args()
        self.argc = len(self.argv)
        self._F_i = '%s_F_i' % self.matrix
        self._F_p = '%s_F_p' % self.matrix
        self._F_ip = '%s_F_ip' % self.matrix
        self._pmi_ip = '%s_pmi_ip' % self.matrix

    def get_args(self):
        '''returns a lists of argument names in <matrix>'''
        x = self.db[self.matrix].find_one()
        return sorted([k 
                       for k in x.keys()
                       if k.startswith('arg')])
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
        with self.db[self.matrix].find() as xs:
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

    def I(self):
        '''returns all instance tuples in <matrix>'''
        with self.db[self.matrix].find(fields=self.argv, timeout=False) as xs:
            for x in xs:
                yield x

    def P(self):
        '''returns all relation patterns in <matrix>'''
        with self.db[self.matrix].find(fields=['rel'], timeout=False):
            for x in xs:
                yield x

    def i2query(self, i):
        '''returns a tuple of argument values labeled (arg1,<value>),
        (arg2,<value>), ..., (argn,<value>) '''
        return SON(zip(self.argv, i))

    def F_i(self, i):
        '''calculate the frequency (i.e. the sum of scores) of an argument 
        instance'''
        try:
            query = SON({'_id':self.i2query(i)})
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
            query = SON({'_id':{'rel':p}})
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
            query = SON({'rel':p})
            query.update(self.i2query(i))
            #print >>sys.stderr, 'query:', query
            #print >>sys.stderr, self.db[self._F_ip].find(query).explain()
            v = self.db[self._F_ip].find_one({'_id':query}, fields=['value'])
            #print >>sys.stderr, 'v:', v
            return v['value']['score']
        except Exception as e:
            print >>sys.stderr, e, i, p
            raise e

    def pmi(self, i, p):
        '''pmi: pointwise mutual information between instance and pattern'''
        return self._pmi(self.F_i(i), self.F_p(p), self.F_ip(i,p))

    def _pmi(self, F_i, F_p, F_ip):
        '''pmi: pointwise mutual information between instance and pattern'''
        return log( F_ip / (F_i*F_p) )

    def max_pmi(self):
        '''max_pmi: maximum PMI between instance and pattern'''
        return max( self.db[self._pmi_ip].find({}, fields=['pmi']) )

    def r_p(self, p):
        '''r_p: reliability of pattern p'''
        return sum( [self.pmi(i,p)*self.r_i(i) /
                     self._max_pmi for i in self._I] ) / len(self._I)

    def r_i(self, i):
        '''r_i: reliability of instance i'''
        return sum( [self.pmi(i,p)*self.r_p(p) /
                     self._max_pmi for p in self._P] ) / len(self._P)

    def smooth(self, x, y=1.0):
        '''returns value of x smoothed by y'''
        return x / (x+y)

    def discount(self, i, p):
        '''discounting factor for PMI towards infrequent elements
        See Equation (2) from:
        Patrick Pantel and Deepak Ravichandran.
        Automatically Labeling Semantic Classes.
        HLT-NAACL 2004.'''
        return self._discount( c_ef=self.F_ip(i,p), c_ei=self.F_i(i), c_jf=self.F_p(p) )

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
        pmi = self._pmi(F_i, F_p, F_ip)
        discount = self._discount(c_ef=F_ip, c_ei=F_i, c_jf=F_p)
        dpmi = pmi*discount
        return dpmi, discount, pmi

def validate_start(s):
    '''maps starting collection name to its order of collection returning 0 if invalid'''
    fs = ('F_i', 'F_p', 'F_pi', 'pmi_ip')
    ns = ('1', '2', '3', '4')
    d = {}
    d.update(zip(fs, xrange(1,5)))
    d.update(zip(ns, xrange(1,5)))
    return d.get(s,0)

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
                              1 or F_i: instance tuple frequencies
                              2 or F_p: relation pattern frequencies
                              3 or F_ip: instance*pattern co-occurence frequencies
                              4 or pmi_ip: instance*pattern Pointwise Mutual Information score
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
    if start <= 1:
        p.make_F_i()
    if start <= 2:
        p.make_F_p()
    if start <= 3:
        p.make_F_ip()
    if start <= 4:
        p.make_pmi_ip()

if __name__ == '__main__':
    main()
