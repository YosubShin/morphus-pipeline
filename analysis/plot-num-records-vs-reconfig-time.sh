#!/bin/bash

for i in "$@"
do
case $i in
    --csv_path=*)
    CSV_PATH="${i#*=}"
    shift
    ;;
    --output_path=*)
    OUTPUT_PATH="${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

gnuplot <<- EOF
set datafile separator ','
set terminal pngcairo font 'Times,20' linewidth 2 size 15in,9in 
set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey
set grid back linestyle 81

set pointsize 2
set output "${OUTPUT_PATH}"
set boxwidth 1.0
set style line 1 lt rgb "#A00000" lw 2 pt 2

set style data histograms
set style histogram errorbars gap 2 lw 2
set style fill pattern 1 border lt -1 

set style line 1 lt rgb "#A00000" pt 1 pi 10 lw 2 ps 1.5
set style line 2 lt rgb "#00A000" pt 9 pi 10 lw 2 ps 1.5
set style line 3 lt rgb "#5060D0" pt 2 pi 10 lw 2 ps 1.5

set xlabel "Data Size (GB)"
set ylabel "Reconfiguration Time (secs)"
set xtics nomirror
set ytics nomirror

set xrange[0:35]
set yrange[:2500]

set datafile separator ","

plot "${CSV_PATH}" using (\$1/1000000):(\$2/1000) with linespoints ls 1 ti "Reconfiguration done", \
     "${CSV_PATH}" using (\$1/1000000):(\$2/1000):(\$3/1000) with yerrorbars ls 1 notitle, \
     "${CSV_PATH}" using (\$1/1000000):(\$4/1000) with linespoints ls 2 ti "Atomic Swap phase done", \
     "${CSV_PATH}" using (\$1/1000000):(\$4/1000):(\$5/1000) with yerrorbars ls 2 notitle, \
     "${CSV_PATH}" using (\$1/1000000):(\$6/1000) with linespoints ls 3 ti "Insert phase done", \
     "${CSV_PATH}" using (\$1/1000000):(\$6/1000):(\$7/1000) with yerrorbars ls 3 notitle

EOF


#set key on right top
#plot "$1.csv" using (\$1/1000000):(\$8/1000) with linespoints ls 1 ti "Reconfiguration done", \
#     '' using (\$1/1000000):(\$8/1000):(\$9/1000) with yerrorbars ls 1 notitle, \
#     '' using (\$1/1000000):(\$2/1000) with linespoints ls 5 ti "Compact phase done", \
#     '' using (\$1/1000000):(\$2/1000):(\$3/1000) with yerrorbars ls 5 notitle, \
#     '' using (\$1/1000000):(\$10/1000) with linespoints ls 3 ti "(Reconfiguration done) - (Compact phase done)", \
#     '' using (\$1/1000000):(\$10/1000):(\$11/1000) with yerrorbars ls 3 notitle

