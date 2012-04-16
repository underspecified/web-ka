#!/usr/bin/python
# -*- coding: utf-8 -*-
################################################################################

'''
matrix2pmi.py: calculates PMI between relation patterns and argument tuples in 
a co-occurence matrix

Creates 4 mongodb collections:

1. <matrix>_f_p
2. <matrix>_f_i
3. <matrix>_f_pi
4. <matrix>_pmi

'''

import pymongo
import sys
from bson.code import Code
from bson.son import SON
from math import log


class PMI:
    def __init__(self, db, matrix, update=False):
        self.db = db
        self.matrix = matrix
        self.update = update
        self.argv = self.get_args()
        self.argc = len(self.argv)
        self._F_i = self.make_F_i()
        self._F_p = self.make_F_p()
        self._F_ip = self.make_F_ip()
        self._pmi_ip = self.make_pmi_ip()

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
                    '}', n=self.argc)
        reduce_ = Code('function (key, values) {'
                       '  var sum = 0;'
                       '  values.forEach('
                       '    function (doc) {sum += doc.score;}'
                       '  );'
                       '  return {score:sum};'
                       '}')
        F_i = '%s_F_i' % self.matrix
        if self.update:
            self.db[self.matrix].map_reduce(
                map_, reduce_, F_i, full_response=True
                )
        print >>sys.stderr, 'making instance counts: done.'
        return F_i

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
        F_p = '%s_F_p' % self.matrix
        if self.update:
            self.db[self.matrix].map_reduce(
                map_, reduce_, F_p, full_response=True
                )
        print >>sys.stderr, 'making pattern counts: done.'
        return F_p

    def make_F_ip(self):
        '''creates a collection <matrix>_F_p containing instance*pattern
        frequencinces and returns its name'''
        print >>sys.stderr, 'making instance*pattern counts...'
        arg_str = ['%s:this.%s'%(a,a) 
                   for a in self.argv]
        map_ = Code('function () {'
                    '  var d = {};'
                    '  for (i=1;i<=n;i++) {d["arg"+i] = this["arg"+i]}'
                    '  d["rel"] = this.rel;'
                    '  emit(d, {score:this.score});'
                    '}', n=self.argc)
        reduce_ = Code('function (key, values) {'
                       '  var sum = 0;'
                       '  values.forEach('
                       '    function (doc) {sum += doc.score;}'
                       '  );'
                       '  return {score:sum};'
                       '}')
        F_ip = '%s_F_ip' % self.matrix
        if self.update:
            self.db[self.matrix].map_reduce(
                map_, reduce_, F_ip, full_response=True
                )
        print >>sys.stderr, 'making instance*pattern counts: done.'
        return F_ip

    def make_pmi_ip(self):
        '''creates a collection <matrix>_pmi_ip containing instance*relation
        Pointwise Mutual Information scores and returns its name'''
        print >>sys.stderr, 'calculating instance*pattern PMI...'
        pmi_ip = '%s_pmi_ip' % self.matrix
        for x in self.db[self.matrix].find():
            i = [x[a] for a in self.argv]
            p = x['rel']
            pmi = self.pmi(i,p)
            x['pmi'] = pmi
            x.pop('_id')
            self.db[pmi_ip].save(x)
        print >>sys.stderr, 'calculating instance*pattern PMI: done.'
        return pmi_ip

    def I(self):
        '''returns all instance tuples in <matrix>'''
        return self.db[self.matrix].find(fields=self.argv)

    def P(self):
        '''returns all relation patterns in <matrix>'''
        return self.db[self.matrix].find(fields=['rel'])

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
            v = self.db[self._F_p].find_one(query, fields=['value'])
            #print >>sys.stderr, 'v:', v
            return v['value']['score']
        except Exception as e:
            print >>sys.stderr, e, i
            raise e

    def F_ip(self, i, p):
        '''calculate the co-occurence frequency (i.e. the sum of scores) of 
        instance*pattern'''        
        try:
            query = self.i2query(i)
            query.update({'rel':p})
            #print >>sys.stderr, 'query:', query
            v = self.db[self._F_ip].find_one({'_id':query}, fields=['value'])
            #print >>sys.stderr, 'v:', v
            return v['value']['score']
        except Exception as e:
            print >>sys.stderr, e, i
            raise e

    def pmi(self, i, p):
        '''pmi: pointwise mutual information between instance and pattern'''
        return log( self.F_ip(i,p) / (self.F_p(p)*self.F_i(i)) )

    def max_pmi(self):
        '''max_pmi: maximum PMI between instance and pattern'''
        return max( self.db[self._pmi_ip].find({}, fields=['pmi']) )

    def r_p(self, p):
        '''r_p: reliability of pattern p'''
        return sum( [self.pmi(i,p)*self.r_i(i) /
                     self._max_pmi for i in self._I] ) / len(I)

    def r_i(self, i):
        '''r_i: reliability of instance i'''
        return sum( [self.pmi(i,p)*self.r_p(p) / 
                     self._max_pmi for p in self._P] ) / len(P)

# def discount():
#     '''discounting factor for PMI towards infrequent elements'''
#     return (C(e,f) / (C(e,f)+1)) * (min( sum([C(e,i) for i in xrange(1,n)]), sum([C(j,f) for i in xrange(1,m)]) ) / (min( sum([C(e,i) for i in xrange(1,n)]), sum([C(j,f) for i in xrange(1,m)]) ) + 1)

def main():
    from optparse import OptionParser
    usage = '''%prog [options] [database] [collection]'''
    parser = OptionParser(usage=usage)
    parser.add_option('-o', '--host', dest='host', default='localhost',
                      help='''mongodb host machine name. default: localhost''')    
    parser.add_option('-p', '--port', dest='port', type=int, default=1979,
                      help='''mongodb host machine port number. default: 1979''')
    parser.add_option('-u', '--update', dest='update',
                      action="store_true", default=False,
                      help='''update co-occurence counts. default: False''')
    options, args = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        exit(1)
    db, collection = args
    connection = pymongo.Connection(options.host, options.port)
    PMI(connection[db], collection, options.update)

if __name__ == '__main__':
    main()
