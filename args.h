// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>
#include <functional>

#include "version.h"
#include "util.h"

using std::string;
using std::vector;
using std::function;

////////////////////////////////////////////////////////////////////////////////////

/*_f(ARG_name, ARG_type, ARG_flag_type, ARG_flag_default, ARG_help, ARG_condition, ARG_set_event)*/
#define ALL_ARGS_Direct_F( _f )                                   \
	_f(log_level, string, DEFINE_string,                          \
		"info",                                                   \
		"Log level (output,debug,info)",                          \
		true,                                                     \
		setLogLevel(value))                                       \
	_f(duration, uint32_t, DEFINE_uint32,                         \
		60,                                                       \
		"Duration of the experiment (minutes) including warm_period", \
		value >= 1,                                               \
		nullptr)                                                  \
	_f(warm_period, uint32_t, DEFINE_uint32,                      \
		0,                                                        \
		"Warm time before the experiment (minutes). Do not report stats during this time.", \
		true,                                                     \
		nullptr)                                                  \
	_f(stats_interval, uint32_t, DEFINE_uint32,                   \
		5,                                                        \
		"Statistics interval (seconds)",                          \
		value > 0,                                                \
		nullptr)                                                  \
	_f(sync_stats, bool, DEFINE_bool,                             \
		true,                                                     \
		"Synchronize statistics whenever possible",               \
		true,                                                     \
		nullptr)                                                  \
	_f(num_dbs, uint32_t, DEFINE_uint32,                          \
		0,                                                        \
		"Number of databases",                                    \
		true,                                                     \
		nullptr)                                                  \
	_f(db_create, bool, DEFINE_bool,                              \
		false,                                                    \
		"Create db_bench database",                               \
		true,                                                     \
		nullptr)                                                  \
	_f(db_mixgraph_params, string, DEFINE_string,                 \
		"--sine_a=1000 --sine_d=4500",                            \
		"Other parameters used in the mixgraph benchmark",        \
		true,                                                     \
		nullptr)                                                  \
	_f(num_ydbs, uint32_t, DEFINE_uint32,                         \
		0,                                                        \
		"Number of YCSB databases",                               \
		true,                                                     \
		nullptr)                                                  \
	_f(ydb_create, bool, DEFINE_bool,                             \
		false,                                                    \
		"Create YCSB database",                                   \
		true,                                                     \
		nullptr)                                                  \
	_f(ydb_rocksdb_jni, string, DEFINE_string,                    \
		"",                                                       \
		"Rocksdb binding used by YCSB.",                          \
		true,                                                     \
		nullptr)                                                  \
	_f(ydb_socket, bool, DEFINE_bool,                             \
		false,                                                    \
		"Activates the socket server for RocksDB's internal statistics. Modified version of YCSB.", \
		true,                                                     \
		nullptr)                                                  \
	_f(rocksdb_config_file, string, DEFINE_string,                \
		"",                                                       \
		"Rocksdb Configuration File",                             \
		true,                                                     \
		nullptr)                                                  \
	_f(num_at, uint32_t, DEFINE_uint32,                           \
		0,                                                        \
		"Number of access_time3 instances",                       \
		true,                                                     \
		nullptr)                                                  \
	_f(docker_image, string, DEFINE_string,                       \
		"alange0001/rocksdb_test:" ROCKSDB_TEST_VERSION,          \
		"docker image used for each container",                   \
		value.length() > 0,                                       \
		nullptr)                                                  \
	_f(docker_params, string, DEFINE_string,                      \
		"",                                                       \
		"additional docker parameters",                           \
		true,                                                     \
		nullptr)                                                  \
	_f(socket, string, DEFINE_string,                             \
		"",                                                       \
		"Socket used to control the experiment",                  \
		value == "" || !std::filesystem::exists(value),           \
		nullptr)                                                  \
	_f(commands, string, DEFINE_string,                           \
		"",                                                       \
		"Commands used to control the experiments",               \
		true,                                                     \
		nullptr)                                                  \
	_f(perfmon, bool, DEFINE_bool,                                \
		false,                                                    \
		"Connect to performancemonitor",                          \
		true,                                                     \
		nullptr)                                                  \
	_f(perfmon_port, uint32_t, DEFINE_uint32,                     \
		18087,                                                    \
		"performancemonitor port",                                \
		value > 0,                                                \
		nullptr)

/*_f(ARG_name, ARG_type, ARG_flag_type, ARG_flag_default, ARG_help, ARG_condition, ARG_set_event, ARG_item_type, ARG_item_condition, ARG_items)*/
#define ALL_ARGS_List_F( _f )                                     \
	_f(db_benchmark, VectorParser<string>, DEFINE_string,         \
		"readwhilewriting",                                       \
		"Database Benchmark (list)",                              \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		value == "readwhilewriting" || value == "readrandomwriterandom" || value == "mixgraph", \
		num_dbs)                                                  \
	_f(db_path, VectorParser<string>, DEFINE_string,              \
		"/media/auto/work/rocksdb",                               \
		"Database Path (list)",                                   \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		value.length() > 0,                                       \
		num_dbs)                                                  \
	_f(db_num_keys, VectorParser<uint64_t>, DEFINE_string,        \
		"50000000",                                               \
		"Number of keys in the database (list)",                  \
		true,                                                     \
		nullptr,                                                  \
		uint64_t,                                                 \
		value > 1000,                                             \
		num_dbs)                                                  \
	_f(db_num_levels, VectorParser<uint32_t>, DEFINE_string,      \
		"6",                                                      \
		"Number of LSM-tree levels in the database (list)",       \
		true,                                                     \
		nullptr,                                                  \
		uint32_t,                                                 \
		value > 2,                                                \
		num_dbs)                                                  \
	_f(db_cache_size, VectorParser<uint64_t>, DEFINE_string,      \
		"268435456",                                              \
		"Database cache size (list)",                             \
		true,                                                     \
		nullptr,                                                  \
		uint64_t,                                                 \
		value >= (1024 * 1024),                                   \
		num_dbs)                                                  \
	_f(db_threads, VectorParser<uint32_t>, DEFINE_string,         \
		"1",                                                      \
		"Database threads (list)",                                \
		value != "",                                              \
		nullptr,                                                  \
		uint32_t,                                                 \
		value >= 1,                                               \
		num_dbs)                                                  \
	_f(db_readwritepercent, VectorParser<uint32_t>, DEFINE_string,\
		"90",                                                     \
		"percent of reads over writes",                           \
		value != "",                                              \
		nullptr,                                                  \
		uint32_t,                                                 \
		(value >= 0)&&(value <= 100),                             \
		num_dbs)                                                  \
	_f(db_sine_cycles, VectorParser<uint32_t>, DEFINE_string,     \
		"1",                                                      \
		"Number of sine cycles in the mixgraph experiment (list)",\
		true,                                                     \
		nullptr,                                                  \
		uint32_t,                                                 \
		value > 0,                                                \
		num_dbs)                                                  \
	_f(db_sine_shift, VectorParser<uint32_t>, DEFINE_string,      \
		"0",                                                      \
		"Shift of sine cycle in minutes (list)",                  \
		true,                                                     \
		nullptr,                                                  \
		uint32_t,                                                 \
		true,                                                     \
		num_dbs)                                                  \
	_f(db_bench_params, VectorParser<string>, DEFINE_string,      \
		"",                                                       \
		"Other parameters used in db_bench (list)",               \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_dbs)                                                  \
	_f(ydb_path, VectorParser<string>, DEFINE_string,             \
		"/media/auto/work/rocksdb",                               \
		"YCSB Database Path (list)",                              \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		value.length() > 0,                                       \
		num_ydbs)                                                 \
	_f(ydb_workload, VectorParser<string>, DEFINE_string,         \
		"",                                                       \
		"YCSB workload file (list)",                              \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_ydbs)                                                 \
	_f(ydb_num_keys, VectorParser<uint64_t>, DEFINE_string,       \
		"50000000",                                               \
		"Number of keys in the database (list)",                  \
		true,                                                     \
		nullptr,                                                  \
		uint64_t,                                                 \
		value > 1000,                                             \
		num_ydbs)                                                 \
	_f(ydb_threads, VectorParser<uint32_t>, DEFINE_string,        \
		"1",                                                      \
		"Number of keys in the database (list)",                  \
		true,                                                     \
		nullptr,                                                  \
		uint32_t,                                                 \
		value >= 1,                                               \
		num_ydbs)                                                 \
	_f(ydb_sleep, VectorParser<uint32_t>, DEFINE_string,          \
		"0",                                                      \
		"Sleep before start (minutes)",                           \
		true,                                                     \
		nullptr,                                                  \
		uint32_t,                                                 \
		value >= 0,                                               \
		num_ydbs)                                                 \
	_f(ydb_params, VectorParser<string>, DEFINE_string,           \
		"",                                                       \
		"Other parameters used in YCSB (list)",                   \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_ydbs)                                                 \
	_f(at_dir, VectorParser<string>, DEFINE_string,               \
		"",                                                       \
		"access_time3 directory mounted inside docker instance (list)", \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		value.length() > 0,                                       \
		num_at)                                                   \
	_f(at_file, VectorParser<string>, DEFINE_string,              \
		"",                                                       \
		"access_time3 --filename (list)",                         \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		value.length() > 0,                                       \
		num_at)                                                   \
	_f(at_block_size, VectorParser<uint32_t>, DEFINE_string,      \
		"4",                                                      \
		"access_time3 --block_size (list)",                       \
		true,                                                     \
		nullptr,                                                  \
		uint32_t,                                                 \
		value >= 4,                                               \
		num_at)                                                   \
	_f(at_io_engine, VectorParser<string>, DEFINE_string,         \
		"",                                                       \
		"access_time3 --io_engine (list)",                        \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_at)                                                   \
	_f(at_iodepth, VectorParser<string>, DEFINE_string,           \
		"",                                                       \
		"access_time3 --iodepth (list)",                          \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_at)                                                   \
	_f(at_o_direct, VectorParser<string>, DEFINE_string,          \
		"",                                                       \
		"access_time3 --o_direct (list)",                         \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_at)                                                   \
	_f(at_o_dsync, VectorParser<string>, DEFINE_string,           \
		"",                                                       \
		"access_time3 --o_dsync (list)",                          \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_at)                                                   \
	_f(at_params, VectorParser<string>, DEFINE_string,            \
		"--random_ratio=0.1 --write_ratio=0.3",                   \
		"other params for the access_time3 (list)",               \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_at)                                                   \
	_f(at_script, VectorParser<string>, DEFINE_string,            \
		"",                                                       \
		"access_time3 --command_script (list)",                   \
		true,                                                     \
		nullptr,                                                  \
		string,                                                   \
		true,                                                     \
		num_at)

#define ALL_ARGS_F( _f )     \
	ALL_ARGS_Direct_F( _f )  \
	ALL_ARGS_List_F( _f )


////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Args::"

struct Args {
	const char*   param_delimiter = "#";

	Args(int argc, char** argv);

#	define declareArg(ARG_name, ARG_type, ...) ARG_type ARG_name;
	ALL_ARGS_F( declareArg );
#	undef declareArg

private:
	void checkUniqueStr(const char* name, const vector<string>& src);
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""
