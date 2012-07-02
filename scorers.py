#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

'''
`scorers.py`: scorers for use in bootstrapping algorithms
'''

import sys

import matrix2pmi
import mongodb

class PrecisionCountScorer:
    __short__ = 'pc'
    def __init__(self, db, matrix, boot_i, boot_p, logger):
        self.db = db
        self.matrix = matrix
        self.boot_i = boot_i
        self.boot_p = boot_p
        self.pmi = matrix2pmi.PMI(db, matrix)
        self.max_pmi = self.pmi.max_pmi()
        self.logger = logger

    def precision_p(self, I, p):
        '''precision is the sum of the number of instances promoted by
        a pattern divided by the count of the pattern'''
        try:
            prec = sum( [self.pmi.F_ip(i,p) for i in I] ) / self.pmi.F_p(p)
            self.logger.info('precision_p: %s %f' % (p, prec))
            return prec
        except Exception as e:
            return 0.0

    def pattern_count(self, i, P):
        '''returns the number of patterns that are promoting an instance'''
        try:
            # pcount = len( filter(lambda x: x>0.0,
            #                      [self.pmi.F_ip(i,p) for p in P]) )
            pcount = sum( [self.pmi.F_ip(i,p) for p in P] )
            self.logger.info('pattern_count: %s %f' % (i, pcount))
            return pcount
        except Exception as e:
            return 0.0        

    def rank_patterns(self, I, P, it):
        '''return a list of patterns ranked by reliability score'''
        rs = [{'rel':p, 'it':it, 'score':self.precision_p(I,p)} 
              for p in P]
        rs.sort(key=lambda r: r.get('score',0.0),reverse=True)
        return rs

    def rank_instances(self, I, P, it):
        '''return a list of instances ranked by reliability score'''
        rs = []
        for i in I:
            r = {'arg%d'%n:v
                 for n,v in enumerate(i, 1)}
            r['it'] = it
            r['score'] = self.pattern_count(i,P)
            rs.append(r)
        rs.sort(key=lambda r: r.get('score',0.0),reverse=True)
        return rs


class ReliabilityScorer:
    '''
    Candidate patterns and instances are ranked by reliability score,
    which reflects the pointwise mutual information score between a
    promoted pattern/instance and the set of instances/patterns that
    generated it.

    (1) r_i(i,P) = sum( dpmi(i,p)*r_p(p) / max_pmi ) / len(P)
                        for p in P

    (2) r_p(P,i) = sum( dpmi(i,p)*r_i(i) / max_pmi ) / len(I)
                        for i in I

    where dpmi is Discounted Pointwise Mutual Information [2].  r_i
    and r_p are recursively defined with r_i=1.0 for the seed instances.
    '''
    __short__ = 'rel'
    def __init__(self, db, matrix, boot_i, boot_p, logger):
        self.db = db
        self.matrix = matrix
        self.boot_i = boot_i
        self.boot_p = boot_p
        self.pmi = matrix2pmi.PMI(db, matrix)
        self.max_pmi = self.pmi.max_pmi()
        self.logger = logger

    def _r_i(self, i):
        '''retrieves r_i for past iteration'''
        try:
            query = mongodb.make_query(i=i,p=None)
            r = self.db[self.boot_i].find_one(query, fields=['score'])
            self.logger.debug('_r_i: %f' % r)
            return r.get('score',0.0)
        except Exception as e:
            return 0.0
        
    def _r_p(self, p):
        '''retrieves r_p for past iteration'''
        try:
            query = mongodb.make_query(i=None,p=p)
            r = self.db[self.boot_p].find_one(query, fields=['score'])
            self.logger.debug('_r_p: %f' % r)
            return r.get('score',0.0)
        except Exception as e:
            return 0.0

    def r_i(self, i, P):
        '''r_i: reliability of instance i'''
        r = sum( [self.pmi.dpmi(i,p)*self._r_p(p) / self.max_pmi 
                  for p in P] ) / len(P)
        self.logger.info('r_i: %s %f' % (i, r))
        return r

    def r_p(self, I, p):
        '''r_p: reliability of pattern p'''
        r = sum( [self.pmi.dpmi(i,p)*self._r_i(i) / self.max_pmi 
                  for i in I] ) / len(I)
        self.logger.info('r_p: %s %f' % (p, r))
        return r

    def S(self, i, P):
        '''confidence in an instance'''
        T = sum ( [ self._r_p(p) for p in P ] )
        return sum ( [ self.pmi.dpmi(i,p)*self._r_p(p)/T for p in P ] )

    def rank_patterns(self, I, P, it):
        '''return a list of patterns ranked by reliability score'''
        rs = [{'rel':p, 'it':it, 'score':self.r_p(I,p)} 
              for p in P]
        rs.sort(key=lambda r: r.get('score',0.0),reverse=True)
        return rs

    def rank_instances(self, I, P, it):
        '''return a list of instances ranked by reliability score'''
        rs = []
        for i in I:
            r = {'arg%d'%n:v
                 for n,v in enumerate(i, 1)}
            r['it'] = it
            r['score'] = self.r_i(i,P)
            rs.append(r)
        rs.sort(key=lambda r: r.get('score',0.0),reverse=True)
        return rs
