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

set style line 2 lc rgb 'black' lt 1 lw 1
set grid y

set style data histograms
set style histogram columnstacked

set boxwidth 0.5
set style fill pattern border -1

set key noinvert box
set yrange [0:700]

set xlabel "Workload type"
set ylabel "Reconfiguration time (sec)"

set tics scale 0.0
set ytics
unset xtics
set xtics norotate nomirror

set datafile separator ","

plot    '${INPUT_PATH}' using 3 ti col, \
        '' using 4 ti col, \
        '' using 5 ti col, \
        '' using 6:key(1) ti col

EOF
