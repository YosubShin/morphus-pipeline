#!/bin/bash

for i in "$@"
do
case $i in
    --output_path=*)
    OUTPUT_PATH="${i#*=}"
    shift
    ;;
    --original=*)
    ORIGINAL="${i#*=}"
    shift
    ;;
    --altered=*)
    ALTERED="${i#*=}"
    shift
    ;;
    --type=*)
    TYPE="${i#*=}"
    shift
    ;;
    --morphus_start_at=*)
    MORPHUS_START_AT="${i#*=}"
    shift
    ;;
    --compact=*)
    COMPACT_MORPHUS_TASK="${i#*=}"
    shift
    ;;
    --insert=*)
    INSERT_MORPHUS_TASK="${i#*=}"
    shift
    ;;
    --atomicswitch=*)
    ATOMIC_SWITCH_MORPHUS_TASK="${i#*=}"
    shift
    ;;
    --catchup=*)
    CATCHUP_MORPHUS_TASK="${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

#MORPHUS_START_AT=0
#COMPACT_MORPHUS_TASK=0
#INSERT_MORPHUS_TASK=0
#ATOMIC_SWITCH_MORPHUS_TASK=0
#CATCHUP_MORPHUS_TASK=80000

gnuplot <<- EOF
set terminal pngcairo font 'Times,20' linewidth 2 size 15in,9in 
set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey
set grid back linestyle 81

set datafile separator ","

set for [i=1:5] linetype i lt i

set output "${OUTPUT_PATH}"
set style line 1 lt 1 lc rgb "#A00000" lw 1
set style line 2 lt 0 lc rgb "#0000A0" lw 1
set ylabel "${TYPE} Latency (ms)"
set xlabel "Time"
set xdata time
set timefmt "%H:%M:%S"
set format x "%H:%M:%S"
# 20 seconds before morphus starts ~ 60 seconds after morphus is over
set xrange[${MORPHUS_START_AT} / 1000 - 20:(${MORPHUS_START_AT} + ${CATCHUP_MORPHUS_TASK}) / 1000 + 60]
set yrange[-11:300]
set xtics rotate by -90 nomirror
set ytics nomirror
set label 4 "Start" rotate by -90
set label 4 at graph (20000.0 / (${CATCHUP_MORPHUS_TASK} + 80000)), 0.80 tc lt 3
set label 2 "Atomic Swap" rotate by -90
set label 2 at graph ((20000.0 + ${INSERT_MORPHUS_TASK}) / (${CATCHUP_MORPHUS_TASK} + 80000)), 0.80 tc lt 3 front
set label 3 "Catch Up" rotate by -90
set label 3 at graph ((20000.0 + ${ATOMIC_SWITCH_MORPHUS_TASK}) / (${CATCHUP_MORPHUS_TASK} + 80000)), 0.80 tc lt 3 front
set label 5 "Finish" rotate by -90
set label 5 at graph ((20000.0 + ${CATCHUP_MORPHUS_TASK}) / (${CATCHUP_MORPHUS_TASK} + 80000)), 0.80 tc lt 3
#unset key

plot "${ORIGINAL}" using (\$1/1000):(\$2/10000) ls 1 title 'Original CF' with line, \
	 "${ALTERED}" using (\$1/1000):(\$2/10000) ls 2 title 'Reconfigured CF' with line
EOF
