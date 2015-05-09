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

#set style data histograms
#set style histogram errorbars gap 2 lw 2
#set style fill pattern 1 border lt -1

set style line 1 lt rgb "#A00000" pt 1 lw 2 ps 1.5
set style line 2 lt rgb "#00A000" pt 9 lw 2 ps 1.5
set style line 3 lt rgb "#5060D0" pt 2 lw 2 ps 1.5

#set xlabel "Data Size (GB)"
#set ylabel "Reconfiguration Time (secs)"

set xtics nomirror
#set ytics nomirror

set ylabel 'Reconfiguration time (sec)'
set y2label 'latencies (ms)'

#set yrange [200:700]
#set y2range [1:800]
set logscale y2

set ytics nomirror
set y2tics
set tics out

set datafile separator ","

plot "${CSV_PATH}" using 1:7 axes x1y1 with linespoints ls 1 ti "Reconfiguration time", \
     "${CSV_PATH}" using 1:(\$10/1000) axes x1y2 with linespoints title 'Update latency 50%', \
     "${CSV_PATH}" using 1:(\$12/1000) axes x1y2 with linespoints title 'Update latency 99%', \
     "${CSV_PATH}" using 1:(\$4/1000) axes x1y2 with linespoints title 'Read latency 50%', \
     "${CSV_PATH}" using 1:(\$6/1000) axes x1y2 with linespoints title 'Read latency 99%'

EOF

#     "${CSV_PATH}" using 1:(\$9/1000):(\$8/1000):(\$12/1000):(\$11/1000) axes x1y2 with candlesticks lt 4 lw 2 title 'Update latency' whiskerbars, \
#     "${CSV_PATH}" using 1:(\$10/1000):(\$10/1000):(\$10/1000):(\$10/1000) axes x1y2 with candlesticks lt -1 lw 2 notitle
#     "${CSV_PATH}" using 1:3:2:6:5 axes x1y2 with candlesticks lt 3 lw 2 title 'Read latency' whiskerbars, \
#     "${CSV_PATH}" using 1:4:4:4:4 axes x1y2 with candlesticks lt -1 lw 2 notitle, \
