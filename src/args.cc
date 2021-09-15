// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "args.h"

#include <string>
#include <stdexcept>
#include <functional>
#include <filesystem>
#include <set>

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <alutils/string.h>
#include <alutils/print.h>

#include "util.h"

using std::string;
using std::runtime_error;
using std::invalid_argument;
using std::function;
using fmt::format;

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""


////////////////////////////////////////////////////////////////////////////////////
#define DEFINE_uint32_t uint32_t
#define DEFINE_uint64_t uint64_t
#define DEFINE_string_t string&
#define DEFINE_bool_t bool
#define declareFlag(ARG_name, ARG_type, ARG_flag_type, ARG_flag_default, ARG_help, ARG_condition, ARG_set_event, ...) \
	ARG_flag_type (ARG_name, ARG_flag_default, ARG_help);                          \
	static bool validate_##ARG_name(const char* flagname, const ARG_flag_type##_t value) { \
		DEBUG_MSG("flagname={}, value={}", flagname, value);              \
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

static string getenv_default(const char* name, const string& default_value) {
	auto env_val = getenv(name);
	if (env_val != nullptr)
		return env_val;
	return default_value;
}

static uint32_t getenv_default(const char* name, const uint32_t default_value) {
	auto env_val = getenv(name);
	if (env_val != nullptr) {
		uint32_t aux = static_cast<uint32_t>(std::atol(env_val));
		return aux;
	}
	return default_value;
}

static bool getenv_default_bool(const char* name, const bool default_value) {
	auto env_val = getenv(name);
	if (env_val != nullptr) {
		string aux(env_val);
		return (aux == "1" || aux == "t" || aux == "true" || aux == "y" || aux == "yes");
	}
	return default_value;
}

static void setLogLevel(const string& value) {
	loglevel.set(value);
}

ALL_ARGS_F( declareFlag );

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Args::"

#define initializeList(ARG_name, ARG_type, ARG_flag_type, ARG_flag_default, ARG_help, ARG_condition, ARG_set_event, ARG_item_type, ARG_item_condition, ARG_items) \
		ARG_name(#ARG_name, param_delimiter, [](const ARG_item_type value)->bool{return (ARG_item_condition);}, &ARG_items),

Args::Args(int argc, char** argv) : ALL_ARGS_List_F(initializeList) log_level("info") {
	gflags::SetUsageMessage(string("\nUSAGE:\n\t") + string(argv[0]) +
				" [OPTIONS]...");
	gflags::ParseCommandLineFlags(&argc, &argv, true);

	string params_str;
#	define printParam(ARG_name, ...) \
		params_str += format("{}--" #ARG_name "=\"{}\"", (params_str.length()>0)?" ":"", FLAGS_##ARG_name);
	ALL_ARGS_F( printParam );
#	undef printParam
	spdlog::info("parameters: {}", params_str);

#	define assignValue(ARG_name, ...) \
		DEBUG_MSG("assign " #ARG_name " = {}", FLAGS_##ARG_name);\
		ARG_name = FLAGS_##ARG_name;
	ALL_ARGS_F( assignValue );
#	undef assignValue

	checkUniqueStr("db_path", db_path);
	checkUniqueStr("ydb_path", ydb_path);
	checkUniqueStr("at_file", at_file);

#	define print_arg(ARG_name, ...) \
		spdlog::info("Args." #ARG_name ": {}", ARG_name);
#	define print_arg_list(ARG_name, ...) \
		for (int i=0; i<ARG_name.size(); i++) \
			spdlog::info("Args." #ARG_name "[{}]: {}", i, ARG_name[i]);
	ALL_ARGS_Direct_F( print_arg );
	ALL_ARGS_List_F( print_arg_list );
#	undef print_arg
#	undef print_arg_list

	checkAtScriptGen();

	if (test_args) // --test_args
		exit(0);
}

void Args::checkUniqueStr(const char* name, const vector<string>& src) {
	for (int i=0; i<src.size(); i++) {
		for (int j=i+1; j<src.size(); j++) {
			if (src[i] == src[j]) throw invalid_argument(format("duplicated entries in {}: {}", name, src[i]));
		}
	}
}

static std::pair<std::string, uint32_t> pressure_scale(Args* args);

void Args::checkAtScriptGen() {
	if (at_script_gen != "") {
		size_t count = 0;
		for (const auto& i: at_script)
			count += i.length();
		if (count > 0)
			throw invalid_argument("-at_script must not be set with -at_script_gen");

		auto script = pressure_scale(this);
		if (duration == 0) {
			duration = script.second;
			spdlog::warn("Redefined Args.duration: {}", duration);
		} else if (duration < script.second) {
			spdlog::warn("duration={} is lesser than the time generated by at_script_gen ({})", duration, script.second);
		}
		at_script = script.first;

		for (int i=0; i<at_script.size(); i++) {
			spdlog::warn("Redefined Args.at_script[{}]: {}", i, at_script[i]);
		}
	} else {
		if (duration == 0)
			throw invalid_argument("invalid value for the parameter duration");
	}
}

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

static std::pair<std::string, uint32_t> pressure_scale(Args* args) {
	DEBUG_MSG("at_script_gen={}, at_script_gen_w0_interval={}, at_script_gen_interval={}",
	          args->at_script_gen, args->at_script_gen_w0_interval, args->at_script_gen_interval);
	auto interval = args->at_script_gen_interval;
	uint32_t wait = args->warm_period;
	while (wait < args->warm_period + args->at_script_gen_w0_interval)
		wait += interval;

	std::vector<std::string> ret;
	uint32_t jc = wait;

	if (args->at_script_gen == "read_to_write") {
		for (uint32_t i=0; i < args->num_at; i++) {
			ret.push_back(format("0:wait;0:write_ratio=0;{}m:wait=false", jc));
			jc += interval;
		}
		for (const auto& wr: std::vector<std::string>{"0.1", "0.2", "0.3", "0.5", "0.7", "1"}) {
			for (uint32_t i=0; i < args->num_at; i++) {
				ret[i] += format(";{}m:write_ratio={}", jc, wr);
				jc += interval;
			}
		}

	} else if (args->at_script_gen == "read_to_write2") { // imported from script_gen 4 of the former rocksdb_test_helper
		for (uint32_t i=0; i < args->num_at; i++) {
			ret.push_back(format("0:wait;0:write_ratio=0;{}m:wait=false", jc));
		}
		jc += interval;
		for (uint32_t i=0; i < args->num_at; i++) {
			ret[i] += format(";{}m:write_ratio=0.1", jc);
		}
		jc += interval;
		for (uint32_t i=0; i < args->num_at; i++) {
			ret[i] += format(";{}m:write_ratio=0.9", jc);
		}
		jc += interval;

	} else if (args->at_script_gen == "active_instances") {
		/* activate one access_time3 instance per interval simulating an increase of iodepth
		 * by using independent instances */
		for (uint32_t i=0; i < args->num_at; i++) {
			ret.push_back(format("0:wait"));
		}
		for (uint32_t i=0; i < args->num_at; i++) {
			ret[i] += format(";{}m:wait=false", jc);
			jc += interval;
		}

	} else if (args->at_script_gen == "iodepth") {
		/* instruct the access_time3 to increase iodepth internally
		 * (require --at_io_engine=X where X is libaio or prwv2) */
		for (auto& i : args->at_io_engine)
			if (i == "")
				i = "libaio"; // set libaio as default
		for (uint32_t i=0; i < args->num_at; i++) {
			ret.push_back(format("0:wait;0:iodepth=1;{}m:wait=false", jc));
		}
		jc += interval;
		for (uint32_t d=2; d <= args->at_script_gen_iodepth_max; d+=args->at_script_gen_iodepth_step) {
			for (uint32_t i=0; i < args->num_at; i++) {
				ret[i] += format(";{}m:iodepth={}", jc, d);
			}
			jc += interval;
		}

	} else {
		throw std::runtime_error(format("invalid pressure name: {}", args->at_script_gen).c_str());
	}

	std::string aux;
	for (const auto& i: ret) {
		DEBUG_MSG("{}", i);
		if (aux.length() > 0)
			aux += "#";
		aux += i;
	}
	DEBUG_MSG("finished. Return: ({}, {})", aux, jc);
	return {aux, jc};
}
