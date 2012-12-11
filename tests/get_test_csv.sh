#!/bin/bash

db=$1
matrix=$2
mkdir -p csv
for b in cpl esp; do
    for r in promotes inhibits necessary part_of source_of; do
	for s in pc rel; do
	    for k in keep nokeep; do
		python instances2csv.py -p 1979 $db ${matrix}_${r}_${b}_i_${s}_${k} > csv/${matrix}_${r}_${b}_i_${s}_${k}.csv
		python patterns2csv.py -p 1979 $db ${matrix}_${r}_${b}_p_${s}_${k} > csv/${matrix}_${r}_${b}_p_${s}_${k}.csv
	    done
	done
    done
done
