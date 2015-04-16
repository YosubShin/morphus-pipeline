#!/bin/sh
MORPHUS_START_AT=$5
COMPACT_MORPHUS_TASK=$6
INSERT_MORPHUS_TASK=$7
ATOMIC_SWITCH_MORPHUS_TASK=$8
CATCHUP_MORPHUS_TASK=$9

gnuplot <<- EOF
set terminal pngcairo font 'Times,20' linewidth 2 size 15in,9in 
set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey
set grid back linestyle 81

set output "$1"
set style line 1 lt rgb "#A00000" lw 1 pt 1
set style line 2 lt rgb "#0000A0" lw 1 pt 1
set ylabel "$2 Latency (ms)"
set xlabel "Time"
set xdata time
set timefmt "%H:%M:%S"
set format x "%H:%M:%S"
# 10 seconds before morphus starts ~ 10 seconds after morphus is over
set xrange[${MORPHUS_START_AT} / 1000 - 10:(${MORPHUS_START_AT} + ${CATCHUP_MORPHUS_TASK}) / 1000 + 10]
set yrange[-5:250]
set xtics rotate by -90 nomirror
set ytics nomirror
set label 4 "Start" rotate by -90
set label 4 at graph (10000.0 / (${CATCHUP_MORPHUS_TASK} + 20000)), 0.95 tc lt 3
set label 1 "Insert" rotate by -90
set label 1 at graph ((10000.0 + ${COMPACT_MORPHUS_TASK}) / (${CATCHUP_MORPHUS_TASK} + 20000)), 0.95 tc lt 3
set label 2 "Atomic Swap" rotate by -90
set label 2 at graph ((10000.0 + ${INSERT_MORPHUS_TASK}) / (${CATCHUP_MORPHUS_TASK} + 20000)), 0.95 tc lt 3 front
set label 3 "Catch Up" rotate by -90
set label 3 at graph ((10000.0 + ${ATOMIC_SWITCH_MORPHUS_TASK}) / (${CATCHUP_MORPHUS_TASK} + 20000)), 0.95 tc lt 3 front
set label 5 "Finish" rotate by -90
set label 5 at graph ((10000.0 + ${CATCHUP_MORPHUS_TASK}) / (${CATCHUP_MORPHUS_TASK} + 20000)), 0.95 tc lt 3
unset key
plot "$3" using (\$1/1000):2 with line ls 1, \
	 "$4" using (\$1/1000):2 with line ls 2
EOF
