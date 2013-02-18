#!/bin/bash

mkdir -p logs
for r in promotes inhibits necessary part_of source_of; do
    (python espresso.py --scorer ReliabilityScorer --reset --start 1 --stop 50 -p 1979 test reverb_clueweb_2 $r seeds/$r.dev 2>&1 | 
	tee logs/esp.$r.rel.keep.$$.log)&
    (python espresso.py --scorer PrecisionCountScorer --reset --start 1 --stop 50 -p 1979 test reverb_clueweb_2 $r seeds/$r.dev 2>&1 | 
	tee logs/esp.$r.pc.keep.$$.log)&
    (python espresso.py --scorer ReliabilityScorer --reset --keep --start 1 --stop 50 -p 1979 test reverb_clueweb_2 $r seeds/$r.dev 2>&1 | 
	tee logs/esp.$r.rel.keep.$$.log)&
    (python espresso.py --scorer PrecisionCountScorer --reset --keep --start 1 --stop 50 -p 1979 test reverb_clueweb_2 $r seeds/$r.dev 2>&1 | 
	tee logs/esp.$r.pc.keep.$$.log)&
done
