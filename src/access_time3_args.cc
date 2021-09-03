// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "access_time3_args.h"

#include <stdexcept>
#include <regex>
#include <filesystem>

#include <gflags/gflags.h>
#include <alutils/string.h>

#include "util.h"

using std::string;
using std::atomic;
using std::deque;
using std::invalid_argument;
using fmt::format;

////////////////////////////////////////////////////////////////////////////////////
#define DEFINE_uint32_t uint32_t
#define DEFINE_uint64_t uint64_t
#define DEFINE_double_t double
#define DEFINE_string_t string&
#define DEFINE_bool_t bool
#define declareFlag(ARG_name, ARG_type, ARG_flag_type, ARG_flag_default, ARG_help, ARG_condition, ARG_set_event, ...) \
	ARG_flag_type (ARG_name, ARG_flag_default, ARG_help);                          \
	static bool validate_##ARG_name(const char* flagname, const ARG_flag_type##_t value) { \
		DEBUG_MSG("flagname={}, value={}", flagname, value);                       \
		if (!(ARG_condition)) {                                                    \
			throw std::invalid_argument(fmt::format(                               \
				"Invalid value for the parameter {}: \"{}\". "                     \
				"Condition: " #ARG_condition ".",                                  \
				flagname, value));                                                 \
		}                                                                          \
		ARG_set_event;                                                             \
		return true;                                                               \
	}                                                                              \
	DEFINE_validator(ARG_name, &validate_##ARG_name);
////////////////////////////////////////////////////////////////////////////////////

static void setLogLevel(const string& value) {
	loglevel.set(value);
}

ALL_ARGS_F( declareFlag );

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "CommandScript::"

CommandScript& CommandScript::operator=(const string& script) {
	DEBUG_MSG("operator=");
	if (script == "")
		return *this;

	std::cmatch cm;

	auto list = alutils::split_str(script, ";");
	for (auto i: list) {
		auto aux = alutils::split_str(i, ":");
		if (aux.size() != 2)
			throw invalid_argument(fmt::format("Invalid command in command_script: {}", i));

		uint64_t time;
		std::regex_search(aux[0].c_str(), cm, std::regex("([0-9]+)([sm]*)$"));
		if (cm.size() < 3)
			throw invalid_argument(fmt::format("Invalid time: {}", aux[0]));

		time = alutils::parseUint64(cm.str(1), true, 0, "invalid time");
		DEBUG_MSG("time_number={}, time_suffix={}, command:{}", cm.str(1), cm.str(2), aux[1]);
		if (cm.str(2) == "m")
			time *= 60;

		this->push_back(CommandLine{time,aux[1]});
	}
	return *this;
}

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "OutputController::"

OutputController::OutputController(out_t output_lambda_) : output_lambda(output_lambda_) {
	debug = (FLAGS_log_level == "debug");
}

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Args::"

Args::Args(int argc, char** argv) {
	gflags::SetUsageMessage(string("\nUSAGE:\n\t") + string(argv[0]) +
				" [OPTIONS]...");
	gflags::ParseCommandLineFlags(&argc, &argv, true);

	string params_str;
#	define printParam(ARG_name, ...) params_str += format("{}--" #ARG_name "=\"{}\"", (params_str.length()>0)?" ":"", FLAGS_##ARG_name);
	ALL_ARGS_F( printParam );
#	undef printParam
	spdlog::info("parameters: {}", params_str);

#	define assignValue(ARG_name, ...) \
		ARG_name = FLAGS_##ARG_name;
	ALL_ARGS_Direct_F( assignValue );
#	undef assignValue

	validate_filename("filename", FLAGS_filename);
	validate_filesize("filesize", FLAGS_filesize);

	if (direct_io) {
		o_direct = true;
		o_dsync = true;
	}

	if (io_engine == "posix" && iodepth > 1) {
		throw invalid_argument("io_engine posix only supports iodepth 1");
	}

	if (FLAGS_log_level == "debug") {
		for (int i=0; i<command_script.size(); i++) {
			spdlog::debug("command_script[{}]: {}:{}", i, command_script[i].time, command_script[i].command);
		}
	}
}

string Args::strStat() {
	string ret;

#	define addArgStr(name) ret += fmt::format("{}\"{}\":\"{}\"", (ret.length()>0) ?", " :"", #name, name)
	addArgStr(wait);
	addArgStr(filesize);
	addArgStr(block_size);
	addArgStr(iodepth);
	addArgStr(flush_blocks);
	addArgStr(write_ratio);
	addArgStr(random_ratio);
#	undef addArgStr

	return ret;
}

void Args::executeCommand(const string& command_line) {
	OutputController oc;
	executeCommand(command_line, oc);
}

#undef DEBUG_F
#define DEBUG_F oc.print_debug

void Args::executeCommand(const string& command_line, OutputController& oc) {
	DEBUG_MSG("command_line: \"{}\"", command_line);

	auto aux = alutils::split_str(command_line, "=");
	string command(aux[0]);
	string value( (aux.size() < 2) ? "" : aux[1].c_str() );

	if (command == "help") {
		oc.print_info(
				"COMMANDS:\n"
				"    stop           - terminate\n"
				"    wait           - (true|false)\n"
				"    block_size     - [4..]\n"
				"    iodepth        - [1..{}]\n"
				"    write_ratio    - [0..1]\n"
				"    random_ratio   - [0..1]\n"
				"    flush_blocks   - [0..]\n"
				, max_iodepth);
		return;
	}

#	define parseLineCommand(name, parser, required, default_) \
		if (command == #name) { \
				name = parser(value, required, default_, "invalid value for the command " #name); \
				oc.print_info("set {}={}", command, name); \
				return; \
		}
#	define parseLineCommandValidate(name, parser, immutable_condition) \
		if (command == #name) { \
				if (immutable_condition) throw invalid_argument("parameter " #name " is immutable due to condition: " #immutable_condition); \
				auto aux = parser(value, true); \
				validate_##name(command.c_str(), aux); \
				name = aux; \
				oc.print_info("set {}={}", command, aux); \
				changed = true; \
				return; \
		}
	parseLineCommand(wait, alutils::parseBool, false, true);
	parseLineCommandValidate(block_size, alutils::parseUint64, false);
	parseLineCommandValidate(iodepth, alutils::parseUint32, io_engine == "posix");
	parseLineCommandValidate(write_ratio, alutils::parseDouble, false);
	parseLineCommandValidate(random_ratio, alutils::parseDouble, false);
	parseLineCommand(flush_blocks, alutils::parseUint64, true, 0);
#	undef parseLineCommand
#	undef parseLineCommandValidate

	throw invalid_argument(fmt::format("Invalid command: {}", command));
}
