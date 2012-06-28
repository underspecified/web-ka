#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

'''
`cpl.py`: an implemenatation of the Coupled Pattern Learning
bootstrapping algorithm

### Usage

        Usage: cpl.py [options] [database] [collection] [rel] [seeds]

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

1. `<matrix>_<rel>_cpl_i`: bootstrapped instances for <rel>
2. `<matrix>_<rel>_cpl_p`: bootstrapped patterns for <rel>

### Bootstrapping

...
'''

import fileinput
import inspect
import multiprocessing
import sys
from ConfigParser import ConfigParser

import scorers
from bootstrapper import Bootstrapper
from multiproc import process_pool_map, process_queue

class CPLWorker(Bootstrapper):
    def __init__(self, host, port, db, matrix, rel,
                 seeds, n, keep, reset, scorer):
        self.boot_i = '%s_%s_cpl_i' % (matrix, rel)
        self.boot_p = '%s_%s_cpl_p' % (matrix, rel)
        Bootstrapper.__init__(
            self, host, port, db, matrix, rel, seeds, n, keep, reset, scorer
            )

    def mutex_pred2patterns(self, pred):
        return 

    def mutex_filter(self, candidates, promoted, mutexes):
        '''filter out all candidate instances that have a co-occurence
        count less than three times the co-occurence count with
        mutually exclusive predicates'''
        def cooc(i, P):
            return sum( [self.pmi.F_ip(i,p) for p in P] )
        return [c for c in candidates 
                if cooc(c, promoted) > 3.0*cooc(c, mutexes)]

def get_scorer(scorer):
    scorers_ = dict(inspect.getmembers(scorers, inspect.isclass))
    return scorers_[scorer]

def iterate(cpl):
    cpl.iterate()

#class CPLManager(multiprocessing.Process):
class CPLManager:
    def __init__(self, config):
        #multiprocessing.Process.__init__(self, name='CPL')
        self.host = config.get('mongo', 'host')
        self.port = config.getint('mongo', 'port')
        self.db = config.get('mongo', 'db')
        self.matrix = config.get('mongo', 'matrix')
        self.scorer = get_scorer(config.get('boot', 'scorer'))
        self.keep = config.get('boot', 'keep')
        self.reset = config.get('boot', 'reset')
        self.n = config.getint('boot', 'n')
        self.rels = config._sections['general']['rels'].split(',')
        self.mutex = {rel:mutex.split(',')
                      for rel, mutex in config._sections['mutex'].items()
                      if rel != '__name__'}
        self.seeds = {rel:(line.strip() for line in open(seed))
                      for rel, seed in config._sections['seeds'].items()
                      if rel != '__name__'}
        cpls = [CPLWorker(self.host, self.port, self.db, self.matrix, rel,
                          self.seeds[rel], self.n, self.keep, self.reset,
                          self.scorer)
                for rel in self.rels]
        #process_pool_map(iterate, cpls)
        #process_queue(iterate, cpls)
        map(iterate, cpls)

def main():
    from optparse import OptionParser
    usage = '''%prog [config.ini]'''
    parser = OptionParser(usage=usage)
    options, args = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        exit(1)
    ini = args[0]
    config = ConfigParser()
    config.read('cpl.ini')
    c = CPLManager(config)

if __name__ == '__main__':
    main()
