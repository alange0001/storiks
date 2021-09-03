// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <deque>
#include <atomic>
#include <functional>

#include <spdlog/spdlog.h>
#include <fmt/format.h>

using std::string;
using std::atomic;
using std::deque;

////////////////////////////////////////////////////////////////////////////////////

const uint32_t max_iodepth = 128;

/*_f(ARG_name, ARG_type, ARG_flag_type, ARG_flag_default, ARG_help, ARG_condition, ARG_set_event)*/
#define ALL_ARGS_Direct_F( _f )                                   \
	_f(log_level, string, DEFINE_string,                          \
		"info",                                                   \
		"Log level (output,debug,info)",                          \
		true,                                                     \
		setLogLevel(value))                                       \
	_f(log_time_prefix, bool, DEFINE_bool,                        \
		true,                                                     \
		"print date and time in each line",                       \
		true,                                                     \
		if (!value) spdlog::set_pattern("[%l] %v"))               \
	_f(socket, string, DEFINE_string,                             \
		"",                                                       \
		"Socket used to control the experiment",                  \
		value == "" || !std::filesystem::exists(value),           \
		nullptr)                                                  \
	_f(duration, uint32_t, DEFINE_uint32,                         \
		0,                                                        \
		"duration time of the experiment (seconds)",              \
		true,                                                     \
		nullptr)                                                  \
	_f(filename, string, DEFINE_string,                           \
		"",                                                       \
		"file name",                                              \
		value.length() != 0,                                      \
		nullptr)                                                  \
	_f(create_file, bool, DEFINE_bool,                            \
		false,                                                    \
		"create file",                                            \
		true,                                                     \
		nullptr)                                                  \
	_f(delete_file, bool, DEFINE_bool,                            \
		false,                                                    \
		"delete file if created",                                 \
		true,                                                     \
		nullptr)                                                  \
	_f(filesize, uint64_t, DEFINE_uint64,                         \
		0,                                                        \
		"file size (MiB)",                                        \
		value >= 10 || !FLAGS_create_file,                        \
		nullptr)                                                  \
	_f(io_engine, string, DEFINE_string,                          \
		"posix",                                                  \
		"I/O engine (posix,prwv2,libaio)",                        \
		value == "posix" || value == "prwv2" || value == "libaio",\
		nullptr)                                                  \
	_f(iodepth, uint32_t, DEFINE_uint32,                          \
		1,                                                        \
		"iodepth",                                                \
		value > 0 && value <= max_iodepth,                        \
		nullptr)                                                  \
	_f(block_size, uint64_t, DEFINE_uint64,                       \
		4,                                                        \
		"block size (KiB)",                                       \
		value >= 4,                                               \
		nullptr)                                                  \
	_f(flush_blocks, uint64_t, DEFINE_uint64,                     \
		0,                                                        \
		"blocks written before a fdatasync (0 = no flush)",       \
		true,                                                     \
		nullptr)                                                  \
	_f(write_ratio, double, DEFINE_double,                        \
		0.0,                                                      \
		"writes/reads ratio (0-1)",                               \
		value >= 0.0 && value <= 1.0,                             \
		nullptr)                                                  \
	_f(random_ratio, double, DEFINE_double,                       \
		0.0,                                                      \
		"random ratio (0-1)",                                     \
		value >= 0.0 && value <= 1.0,                             \
		nullptr)                                                  \
	_f(direct_io, bool, DEFINE_bool,                              \
		false,                                                    \
		"same that -o_direct -o_dsync (backward compatibility)",  \
		true,                                                     \
		nullptr)                                                  \
	_f(o_direct, bool, DEFINE_bool,                               \
		true,                                                     \
		"use O_DIRECT",                                           \
		true,                                                     \
		nullptr)                                                  \
	_f(o_dsync, bool, DEFINE_bool,                                \
		false,                                                    \
		"use O_DSYNC",                                            \
		true,                                                     \
		nullptr)                                                  \
	_f(stats_interval, uint32_t, DEFINE_uint32,                   \
		5,                                                        \
		"Statistics interval (seconds)",                          \
		value > 0,                                                \
		nullptr)                                                  \
	_f(wait, bool, DEFINE_bool,                                   \
		false,                                                    \
		"wait",                                                   \
		true,                                                     \
		nullptr)                                                  \
	_f(command_script, CommandScript, DEFINE_string,              \
		"",                                                       \
		"Script of commands. Syntax: \"time1:command1=value1,time2:command2=value2\"", \
		true,                                                     \
		nullptr)

#define ALL_ARGS_F( _f )     \
	ALL_ARGS_Direct_F( _f )

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "CommandLine::"
struct CommandLine {
	uint64_t    time=0;
	string command;
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "CommandScript::"
class CommandScript : public deque<CommandLine> {
public:
	CommandScript& operator=(const string& script);
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "OutputController::"

class OutputController {
	public:
	typedef std::function<void(const std::string&)> out_t;

	private:
	bool debug = false;
	out_t output_lambda = nullptr;

	public:
	OutputController(out_t output_lambda_ = nullptr);

	template<typename... Types>
	void print_debug(const Types&... args) {
		if (!debug) return;
		if (output_lambda == nullptr)
			spdlog::debug(args...);
		else {
			output_lambda(string("DEBUG: ") + fmt::format(args...));
		}
	}

	template<typename... Types>
	void print_info(const Types&... args) {
		if (output_lambda == nullptr)
			spdlog::info(args...);
		else {
			output_lambda(fmt::format(args...));
		}
	}

	template<typename... Types>
	void print_warn(const Types&... args) {
		if (output_lambda == nullptr)
			spdlog::warn(args...);
		else {
			output_lambda(string("WARN: ") + fmt::format(args...));
		}
	}

	template<typename... Types>
	void print_error(const Types&... args) {
		if (output_lambda == nullptr)
			spdlog::error(args...);
		else {
			output_lambda(string("ERROR: ") + fmt::format(args...));
		}
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Args::"

struct Args {
	bool changed = false;

#	define declareArg(ARG_name, ARG_type, ...) ARG_type ARG_name;
	ALL_ARGS_F( declareArg );
#	undef declareArg

	Args(int argc, char** argv);
	void executeCommand(const string& command_line);
	void executeCommand(const string& command_line, OutputController& oc);
	string strStat();
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""
