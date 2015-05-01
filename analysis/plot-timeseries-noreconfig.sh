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

set output "${OUTPUT_PATH}"
set style line 1 lt rgb "#A00000" lw 1 pt 1
set style line 2 lt rgb "#0000A0" lw 1 pt 1
set ylabel "${TYPE} Latency (ms)"
set xlabel "Time"
set xdata time
set timefmt "%H:%M:%S"
set format x "%H:%M:%S"
# 10 seconds before morphus starts ~ 10 seconds after morphus is over
set xrange[5:65]
set yrange[-11:380]
set xtics rotate by -90 nomirror
set ytics nomirror

unset key
plot "${ORIGINAL}" using (\$1/1000):(\$2/10000) title 'Original CF' with line ls 2

EOF
