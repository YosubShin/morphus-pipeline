#!/bin/bash

for i in "$@"
do
case $i in
    --output_path=*)
    OUTPUT_PATH="${i#*=}"
    shift
    ;;
    --input_path=*)
    INPUT_PATH="${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

gnuplot << EOF
set terminal pngcairo font 'Times,20' linewidth 2 size 15in,9in 
set output "${OUTPUT_PATH}"

set style histogram columnstacked
set key noinvert box
set yrange [0:*]

set xlabel "Workload type"
set ylabel "Phase duration (ms)"

set tics scale 0.0
set ytics
unset xtics
set xtics norotate nomirror

set datafile separator ","

plot    '${INPUT_PATH}' using 6 ti col, \
        '' using 12 ti col, \
        '' using 13 ti col, \
        '' using 14:key(1) ti col

EOF
