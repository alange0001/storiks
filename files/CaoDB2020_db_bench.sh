#!/bin/bash

benchmark="$1"
hours="$2"
[ -z "$hours" ] && hours=2

options_file=rocksdb.options
db=/mnt/work/rocksdb
cache_size=268435456

num_keys=50000000

function getDuration() {
	local hours="$1"
	echo "--duration=`echo "$hours * 60^2" |bc`"
	echo "--sine_a=1000"
	echo "--sine_b=`echo "0.000073 * (24/$hours)" |bc`"
	echo "--sine_d=4500"
}

DURATION="`getDuration $hours`"

if [ "$benchmark" == 'create' ]; then
	db_bench                           \
		--options_file="$options_file" \
		--benchmarks=fillrandom        \
		--perf_level=3                 \
		--use_direct_io_for_flush_and_compaction=true \
		--use_direct_reads=true        \
		--cache_size="$cache_size"     \
		--key_size=48                  \
		--value_size=43                \
		--num="$num_keys"              \
		--db="$db"
elif [ "$benchmark" == 'prefix_dist' ]; then
	db_bench                           \
		--db="$db"                     \
		--use_existing_db=true         \
		--options_file="$options_file" \
		--num="$num_keys"              \
		--key_size=48                  \
		--perf_level=2                 \
		--stats_interval_seconds=5     \
		--stats_per_interval=1         \
		--benchmarks=mixgraph          \
		--use_direct_io_for_flush_and_compaction=true \
		--use_direct_reads=true        \
		--cache_size="$cache_size"     \
		--key_dist_a=0.002312          \
		--key_dist_b=0.3467            \
		--keyrange_dist_a=14.18        \
		--keyrange_dist_b=-2.917       \
		--keyrange_dist_c=0.0164       \
		--keyrange_dist_d=-0.08082     \
		--keyrange_num=30              \
		--value_k=0.2615               \
		--value_sigma=25.45            \
		--iter_k=2.517                 \
		--iter_sigma=14.236            \
		--mix_get_ratio=0.83           \
		--mix_put_ratio=0.14           \
		--mix_seek_ratio=0.03          \
		--sine_mix_rate_interval_milliseconds=5000 \
		$DURATION
fi
