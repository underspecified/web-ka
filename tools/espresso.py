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
import inspect
import logging
import sys

from bootstrapper import Bootstrapper
import scorers


class Espresso(Bootstrapper):
    __short__ = 'esp'
    def __init__(self, host, port, db, matrix, rel, seeds, n, keep, reset,
                 scorer, it=1):
        #logging.basicConfig()
        self.logger = logging.getLogger('Espresso')
        self.logger.setLevel(logging.INFO)
        #self.logger.setLevel(logging.WARNING)
        if len(self.logger.handlers) == 0:
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            fmt = '%(asctime)s [%(levelname)s/Espresso:' + rel + '] %(message)s'
            formatter = logging.Formatter(fmt)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        Bootstrapper.__init__(
            self, host, port, db, matrix, rel, 
            seeds, n, keep, reset, scorer, it
            )

def main():
    scorers_ = dict(inspect.getmembers(scorers, inspect.isclass))
    from optparse import OptionParser
    usage = '''%prog [options] [database] [collection] [rel] [seeds]'''
    parser = OptionParser(usage=usage)
    parser.add_option('-k', '--keep-seeds',
                      action='store_true', dest='keep', default=False,
                      help='''keep seeds and acquired items and use for candidate selection. default: False''')        
    parser.add_option('-n', '--n-best', dest='n', type=int, default=10,
                      help='''number of candidates to keep per iteration. default: 10''')    
    parser.add_option('-o', '--host', dest='host', default='localhost',
                      help='''mongodb host machine name. default: localhost''')    
    parser.add_option('-p', '--port', dest='port', type=int, default=27017,
                      help='''mongodb host machine port number. default: 27017''')
    parser.add_option('-r', '--reset',
                      action='store_true', dest='reset', default=False,
                      help='''reset bootstrapping results. default: False''')
    parser.add_option('--scorer', dest='scorer',
                      choices=scorers_.keys(), default='ReliabilityScorer',
                      help='''scoring method to use''')
    parser.add_option('-s', '--start', dest='start', type=int, default=1,
                      help='''iteration to start with. default: 1''')
    parser.add_option('-t', '--stop', dest='stop', type=int, default=10,
                      help='''iteration to stop at. default: 10''')
    options, args = parser.parse_args()
    if len(args) < 3:
        parser.print_help()
        exit(1)
    db, matrix, rel = args[:3]
    files = args[3:]
    seeds = [i.strip() for i in fileinput.input(files)]
    scorer = scorers_[options.scorer]
    e = Espresso(options.host, options.port, db, matrix, rel, seeds, 
                 options.n, options.keep, options.reset, scorer, 
                 options.start)
    e.bootstrap(options.start, options.stop)

if __name__ == '__main__':
    main()
