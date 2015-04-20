#!/bin/bash

for i in "$@"
do
case $i in
    --output_path=*)
    OUTPUT_PATH="${i#*=}"
    shift
    ;;
    --readonly=*)
    READONLY="${i#*=}"
    shift
    ;;
    --uniform=*)
    UNIFORM="${i#*=}"
    shift
    ;;
    --zipfian=*)
    ZIPFIAN="${i#*=}"
    shift
    ;;
    --latest=*)
    LATEST="${i#*=}"
    shift
    ;;
    --noreconfig=*)
    NORECONFIG="${i#*=}"
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

set xlabel "Read Latency (ms)"
set ylabel "Probability"
set style line 1 lt rgb "#A00000" pt 1 pi 100 lw 2 ps 2
set style line 2 lt rgb "#00A000" pt 7 pi 100 lw 2 ps 2
set style line 3 lt rgb "#5060D0" pt 2 pi 100 lw 2 ps 2
set style line 4 lt rgb "#F25900" pt 9 pi 100 lw 2 ps 2
set style line 5 lt rgb "#ED0CCB" pt 5 pi 100 lw 2 ps 2
set   autoscale                        # scale axes automatically
set logscale x
set xtic auto                          # set xtics automatically
set ytic auto                          # set ytics automatically
set yrange [0:1]
set xrange [1:10]
set key right bottom

set datafile separator ","

plot    "${READONLY}" using 1:2 title 'No Write' with linespoints ls 1, \
        "${UNIFORM}" using 1:2 title 'Uniform' with linespoints ls 2, \
    	"${LATEST}" using 1:2 title 'Latest' with linespoints ls 3, \
    	"${ZIPFIAN}" using 1:2 title 'Zipf' with linespoints ls 4, \
    	"${NORECONFIG}" using 1:2 title 'No Reconfiguration' with linespoints ls 5

EOF


#plot   	"$2" using 1:2 title 'No Reconf' with linespoints ls 5, \
