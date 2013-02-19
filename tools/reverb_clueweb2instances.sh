#!/usr/bin/bash

if [[ $# -ne 1 -a -z "$3"]]; then
    echo "usage: reverb_clueweb2_instances.sh <source-file> > <target-file>"
    exit 1
fi

file=$1

cut -f1-6 $file |
awk '
BEGIN {
    FS = OFS = "\t"
}
{
    score = 1.0
    argc = 2
    arg1 = $2
    rel = $3
    arg2 = $4

    print score, source, rel, argc, arg1, arg2
}' source=$(basename $file)
