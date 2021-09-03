// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rcm.h"

#include <alutils/print.h>
#include <alutils/string.h>
#include <alutils/socket.h>
#include <nlohmann/json.hpp>

#include <sstream>

namespace RCM {

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

#define RCM_PRINT_F(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)
#define RCM_DEBUG_CONDITION debug

#define RCM_DEBUG(format, ...) if (RCM_DEBUG_CONDITION) {RCM_PRINT_F("DEBUG %s%s() [%d]: " format, __CLASS__, __func__, __LINE__, ##__VA_ARGS__);}
#define RCM_ERROR(format, ...) RCM_PRINT_F("RCM ERROR: " format, ##__VA_ARGS__)
#define RCM_PRINT(format, ...) RCM_PRINT_F("RCM: " format, ##__VA_ARGS__)
#define RCM_REPORT(format, ...) RCM_PRINT_F("RCM REPORT: " format, ##__VA_ARGS__)

#define RCM_DEBUG_SM(...)

#define v2s(val) std::to_string(val).c_str()

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "RCM::Env::"

struct Env {
	const char*  envname_debug    = "ROCKSDB_RCM_DEBUG";
	const char*  envname_socket   = "ROCKSDB_RCM_SOCKET";
	const char*  envname_interval              = "ROCKSDB_RCM_INTERVAL";
	const char*  envname_interval_map          = "ROCKSDB_RCM_INTERVAL_MAP";
	const char*  envname_interval_cfname       = "ROCKSDB_RCM_INTERVAL_CFNAME";
	const char*  envname_interval_properties   = "ROCKSDB_RCM_INTERVAL_PROPERTIES";
	const char*  envname_interval_cfproperties = "ROCKSDB_RCM_INTERVAL_CFPROPERTIES";

	bool         debug = false;
	std::string  socket = "";
	int          interval = 0;
	std::string  interval_cfname = "";
	bool         interval_map = false;
	std::vector<std::string> interval_properties;
	std::vector<std::string> interval_cfproperties;

	Env() {
		const char* eaux;
		eaux = getenv(envname_debug);
		if (eaux != nullptr && eaux[0] == '1') {
			debug = true;
		}
		RCM_DEBUG("debug = %s", debug?"true":"false");
		eaux = getenv(envname_socket);
		if (eaux != nullptr && eaux[0] != '\0') {
			socket = eaux;
		}
		RCM_DEBUG("socket = %s", socket.c_str());
		eaux = getenv(envname_interval);
		if (eaux != nullptr) {
			auto aux2 = std::atoi(eaux);
			if (aux2 >=1)
				interval = aux2;
			else
				RCM_ERROR("invalid value for the environment variable %s: %d", envname_interval, aux2);
		}
		RCM_DEBUG("interval = %s", v2s(interval));
		eaux = getenv(envname_interval_map);
		if (eaux != nullptr && eaux[0] == '1') {
			interval_map = true;
		}
		RCM_DEBUG("interval_map = %s", interval_map?"true":"false");
		eaux = getenv(envname_interval_cfname);
		if (eaux != nullptr && eaux[0] != '\0') {
			interval_cfname = eaux;
		}
		RCM_DEBUG("interval_cfname = %s", interval_cfname.c_str());
		eaux = getenv(envname_interval_properties);
		if (eaux != nullptr && eaux[0] != '\0'){
			interval_properties = ROCKSDB_NAMESPACE::StringSplit(eaux, ',');
		}
		RCM_DEBUG("interval_properties = %s", eaux);
		eaux = getenv(envname_interval_cfproperties);
		if (eaux != nullptr && eaux[0] != '\0'){
			interval_cfproperties = ROCKSDB_NAMESPACE::StringSplit(eaux, ',');
		}
		RCM_DEBUG("interval_cfproperties = %s", eaux);
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "RCM::OutputHandler::"

class OutputHandler {
	public:
	bool debug = false;
	bool output_socket = false;
	bool output_stderr = true;
	alutils::Socket::HandlerData* data = nullptr;

	OutputHandler() {}
	OutputHandler(Env& env) : debug(env.debug) {}

	void print(const char* format, ...) {
		va_list args;
		va_start(args, format);
		std::string msg = alutils::vsprintf(format, args);
		va_end(args);
		if (output_socket && data != nullptr) {
			data->send((msg + "\n").c_str(), false);
		}
		if (output_stderr) {
			fprintf(stderr, "%s\n", msg.c_str());
		}
	}
};

class OutputLogger: public ROCKSDB_NAMESPACE::Logger {
	OutputHandler* output = nullptr;

	public:
	explicit OutputLogger(OutputHandler* output_)
	      : Logger(ROCKSDB_NAMESPACE::InfoLogLevel::INFO_LEVEL), output(output_) {}

    using ROCKSDB_NAMESPACE::Logger::Logv;

	void Logv(const char* format, va_list arg_list) override {
		if (output != nullptr) {
			std::string aux(alutils::vsprintf(format, arg_list));
			output->print("%s", aux.c_str());
		}
	}
};

#undef RCM_PRINT_F
#define RCM_PRINT_F(format, ...) output.print(format, ##__VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "RCM::CommandLine::"

struct CommandLine {
	OutputHandler output;
	bool debug = false;
	bool valid = false;
	std::string command_line;
	std::string command;
	std::string params_str;
	std::map<std::string, std::string> params;
	std::map<std::string, std::string> tags_before;
	std::map<std::string, std::string> tags;

	CommandLine(Env& env, alutils::Socket::HandlerData* data) : output(env), debug(env.debug) {
		std::string line = data->msg;
		std::smatch sm;
		output.data = data;
		output.output_socket = (data != nullptr);
		output.output_stderr = (data == nullptr);

		std::regex_search(line, sm, std::regex("^(.+)"));
		RCM_DEBUG_SM(sm);
		if (sm.size() < 2) return;
		command_line = sm.str(1);
		if (command_line.length() == 0) return;

		std::regex_search(command_line, sm, std::regex("^(\\w+)(|\\s+.+)$"));
		RCM_DEBUG_SM(sm);
		if (sm.size() < 3) return;
		command = sm.str(1);
		params_str = sm.str(2);
		std::string tail = params_str;

		const std::vector<std::string> regex_list = {
				"([\\w.:-_]+)\\s*=\\s*'([^']+)'",
				"([\\w.:-_]+)\\s*=\\s*\"([^\"]+)\"",
				"([\\w.:-_]+)\\s*=\\s*(\\w+)"}; // TODO possible problems special characters
		while (true) {
			bool found = false;
			for (const auto &r : regex_list) {
				std::regex_search(tail, sm, std::regex(r.c_str()));
				RCM_DEBUG_SM(sm);
				if (sm.size() >= 3) {
					std::string key = sm.str(1);
					std::string value = sm.str(2);
					RCM_DEBUG("param parsed: %s = %s", key.c_str(), value.c_str());
					tail.replace(tail.find(sm.str(0)), sm.str(0).length(), "");

					std::regex_search(key, sm, std::regex("(tag|tag_before)\\.(\\w+)")); // tags

					if (key == "output") {
						const std::set<std::string> stderr_vals  = {"stderr", "both", "all"};
						const std::set<std::string> socket_vals  = {"socket", "both", "all"};
						output.output_stderr = (stderr_vals.count(value) > 0);
						output.output_socket = (socket_vals.count(value) > 0) || !output.output_stderr;
						RCM_DEBUG("output_socket = %s, output_stderr = %s", v2s(output.output_socket), v2s(output.output_stderr));
					} else if (key == "debug") {
						const std::set<std::string> true_vals  = {"1", "yes", "true"};
						const std::set<std::string> false_vals = {"0", "no",  "false"};
						if (true_vals.count(value) > 0) {
							output.debug = true;
						} else if (false_vals.count(value) > 0) {
							output.debug = false;
						} else {
							RCM_ERROR("invalid debug value: %s", value.c_str());
						}
						debug = output.debug;
						RCM_DEBUG("debug = %s", v2s(debug));
					} else if (sm.size() >= 3) { // tags
						if (sm.str(1) == "tag_before") {
							tags_before[sm.str(2)] = value;
							RCM_DEBUG("tags_before[%s] = %s", sm.str(2).c_str(), value.c_str());
						} else {
							tags[sm.str(2)] = value;
							RCM_DEBUG("tags[%s] = %s", sm.str(2).c_str(), value.c_str());
						}
					} else {
						params[key] = value;
						RCM_DEBUG("params[%s] = %s", key.c_str(), value.c_str());
					}

					found = true;
					break;
				}
			}
			if (!found) break;
		}

		valid = true;
	}

	bool operator== (bool val) {
		return val == valid;
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "RCM::ControllerImpl::"

class ControllerImpl : public Controller {
	Env env;
	OutputHandler output;
	bool     debug  = false;
	bool     stop_  = false;
	bool     active = false;
	ROCKSDB_NAMESPACE::DB*      db;
	std::map<std::string, ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cfmap;
	std::unique_ptr<alutils::Socket>           socket_server;

	std::map<std::string, std::string> tags;

	public: //-----------------------------------------------------------------

	ControllerImpl(): db(nullptr) {
		throw std::runtime_error("invalid constructor");
	}

	ControllerImpl(ROCKSDB_NAMESPACE::DB *db_, std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>* handles) : db(db_) {
		debug = env.debug;
		output = RCM::OutputHandler(env);
		if (debug) {
			alutils::log_level = alutils::LOG_DEBUG;
		}
		RCM_DEBUG("constructor begin");

		if (handles != nullptr) {
			for (auto h : (*handles)){
				if (h != nullptr) {
					auto h_cfname = h->GetName();
					RCM_PRINT("registering column family: %s", h_cfname.c_str());
					cfmap[h_cfname] = h;
				}
			}
		}

		if (env.socket.length() > 0) {
			auto handler_l = [this](alutils::Socket::HandlerData* data)->void{socket_handler(data);};
			alutils::Socket::Params p; p.buffer_size=4096;
			socket_server.reset(new alutils::Socket(
					alutils::Socket::tServer,
					env.socket.c_str(),
					handler_l,
					p
			));
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
		}

		if (env.interval > 0) {
			RCM_PRINT("initiating report interval thread");
			std::thread thread = std::thread(&ControllerImpl::thread_main, this);
			thread.detach();
		}
		RCM_DEBUG("constructor end");
	}

	~ControllerImpl() {
		stop_ = true;
		RCM_DEBUG("destructor begin");

		socket_server.reset(nullptr);

		for (int i=0; i < 20 && active; i++){
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		RCM_DEBUG("destructor end");
	}

	private: //----------------------------------------------------------------

	void thread_main() noexcept {
		active = true;
		try {
			ROCKSDB_NAMESPACE::ColumnFamilyHandle* cfhandle = nullptr;
			if (env.interval_cfname != "") {
				if (cfmap.count(env.interval_cfname) > 0) {
					cfhandle = cfmap[env.interval_cfname];
				} else {
					env.interval_cfname = "";
				}
			}

			std::this_thread::sleep_for(std::chrono::milliseconds(200));

			int c = 0;
			while (! stop_) {
				c++;
				if (c > env.interval*5) {
					c = 0;
					for (auto s : env.interval_properties) {
						std::string stats;
						RCM_REPORT("");
						RCM_REPORT("==========================================");
						RCM_REPORT("BEGIN %s:", s.c_str());
						if (env.interval_map) {
							std::map<std::string, std::string> mstats;
							if (db->GetMapProperty(s.c_str(), &mstats)) {
								for (auto const& ist : mstats) {
									RCM_REPORT("%s :\t%s", ist.first.c_str(), ist.second.c_str());
								}
							}
						} else {
							if (db->GetProperty(s.c_str(), &stats)) {
								std::stringstream stats_stream(stats);
								std::string line;
								while( std::getline(stats_stream, line) ) {
									RCM_REPORT("%s", line.c_str());
								}
							}
						}
						RCM_REPORT("END %s:", s.c_str());
					}
					if (stop_) break;

					if (env.interval_cfname.length() > 0) {
						for (auto s : env.interval_cfproperties) {
							RCM_REPORT("");
							RCM_REPORT("==========================================");
							RCM_REPORT("BEGIN %s, COLUMN FAMILY %s:", s.c_str(), cfhandle->GetName().c_str());
							if (env.interval_map) {
								std::map<std::string, std::string> mstats;
								if (db->GetMapProperty(cfhandle, s.c_str(), &mstats)) {
									for (auto const& ist : mstats) {
										RCM_REPORT("%s :\t%s", ist.first.c_str(), ist.second.c_str());
									}
								}
							} else {
								std::string stats;
								if (db->GetProperty(cfhandle, s.c_str(), &stats)) {
									std::stringstream stats_stream(stats);
									std::string line;
									while( std::getline(stats_stream, line) ) {
										RCM_REPORT("%s", line.c_str());
									}
								}
							}
							RCM_REPORT("END %s, COLUMN FAMILY %s:", s.c_str(), cfhandle->GetName().c_str());
						}
					}
					if (stop_) break;
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(200));
			}
		} catch (const std::exception& e) {
			RCM_ERROR("report interval exception: %s", e.what());
		}
		RCM_PRINT("report interval thread finished");
		active = false;
	}

#	undef  RCM_PRINT_F
#	define RCM_PRINT_F(format, ...) cmd.output.print(format, ##__VA_ARGS__)
#	undef  RCM_DEBUG_CONDITION
#	define RCM_DEBUG_CONDITION cmd.output.debug

	std::string last_command;
	uint32_t    last_command_count = 0;
	bool        last_command_sucess = true;

	void socket_handler(alutils::Socket::HandlerData* data) {
		CommandLine cmd(env, data);
		RCM_DEBUG("message received: %s", data->msg.c_str());
		RCM_DEBUG("cmd.valid = %s", v2s(cmd.valid));

		if (cmd.valid) {
			for (const auto &i : cmd.tags_before) {
				tags[i.first] = i.second;
				RCM_DEBUG("tags[%s] = %s", i.first.c_str(), i.second.c_str());
			}

			bool handled = false;
			bool success = false;
			bool update_last = false;

#			define handle_cmd(name, ulast) if (!handled && cmd.command == #name) {handled = true; update_last = ulast; success = handle_##name(cmd);}
			handle_cmd(report,         false);
			handle_cmd(metadata,       false);
			handle_cmd(listproperties, false);
			handle_cmd(getproperty,    false);
			handle_cmd(getoptions,     false);
			handle_cmd(setoptions,     true);
			handle_cmd(setdboptions,   true);
			handle_cmd(compact_level,  true);
			handle_cmd(test,           false);
#			undef handle_cmd

			RCM_DEBUG("handled = %s, success = %s", v2s(handled), v2s(success));

			if (handled) {
				if (success) {
					for (const auto &i : cmd.tags) {
						tags[i.first] = i.second;
						RCM_DEBUG("tags[%s] = %s", i.first.c_str(), i.second.c_str());
					}
				}
				if (update_last) {
					last_command = cmd.command_line;
					last_command_count++;
					last_command_sucess = success;
				}
			} else {
				RCM_ERROR("command not found: %s", cmd.command.c_str());
			}

		} else {
			RCM_ERROR("invalid socket command line: %s", data->msg.c_str());
		}
	}

	ROCKSDB_NAMESPACE::ColumnFamilyHandle* get_cfhandle(CommandLine& cmd) {
		ROCKSDB_NAMESPACE::ColumnFamilyHandle* cfhandle = db->DefaultColumnFamily();
		std::string cfname(cmd.params.count("column_family") > 0 ? cmd.params["column_family"] : "");
		if (cfname != "") {
			if (cfmap.count(cfname) > 0) {
				cfhandle = cfmap[cfname];
			} else {
				RCM_ERROR("invalid column family name: %s", cfname.c_str());
				return nullptr;
			}
		}
		return cfhandle;
	}

	bool handle_report(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		nlohmann::ordered_json rep;
		std::string aux;
		std::map<std::string, std::string> mstats;
		std::string stats_name = "rocksdb.cfstats";

		std::string column_family = cmd.params["column_family"];

		if (column_family == "") {
			RCM_DEBUG("reading database statistics: %s", stats_name.c_str());
			if (! db->GetMapProperty(stats_name.c_str(), &mstats)) {
				RCM_ERROR("failed to retrieve %s", stats_name.c_str());
				return false;
			}
			rep[stats_name] = mstats;

		} else {
			ROCKSDB_NAMESPACE::ColumnFamilyHandle* cfhandle = nullptr;
			if (cfmap.count(column_family) > 0) {
				cfhandle = cfmap[column_family];
			} else {
				RCM_ERROR("column_family=\"%s\" not found", column_family.c_str());
				return false;
			}
			rep["column_family"] = column_family;
			RCM_DEBUG("reading database statistics: %s, column_family=%s", stats_name.c_str(), column_family.c_str());
			if (! db->GetMapProperty(cfhandle, stats_name.c_str(), &mstats)) {
				RCM_ERROR("failed to retrieve %s from column_family=%s", stats_name.c_str(), column_family.c_str());
				return false;
			}
			rep[stats_name] = mstats;
		}

		rep["tag"] = tags;
		rep["last_command"] = last_command;
		rep["last_command_count"] = last_command_count;
		rep["last_command_status"] = last_command_sucess ? "success" : "fail";

		RCM_DEBUG("reporting stats");
		RCM_REPORT("socket_server.json: %s", rep.dump().c_str());
		return true;
	}

	bool handle_listproperties(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		std::string ret;

		const auto& stats_info = ROCKSDB_NAMESPACE::InternalStats::ppt_name_to_info;
		for (const auto& i: stats_info) {
			ret += i.first + " "
				+  ((i.second.handle_string        != nullptr) ? "(str)" : "")
				+  ((i.second.handle_int           != nullptr) ? "(int)" : "")
				+  ((i.second.handle_map           != nullptr) ? "(map)" : "")
				+  ((i.second.handle_string_dbimpl != nullptr) ? "(str_dbimpl)" : "")
				+ "\n";
		}

		RCM_REPORT("Property list:\n%s", ret.c_str());
		return true;
	}

	bool handle_getproperty(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		auto cfhandle = get_cfhandle(cmd);
		if (cfhandle == nullptr) return false;
		std::string cfname(cmd.params.count("column_family") > 0 ? cmd.params["column_family"] : "default");
		std::string name(cmd.params.count("name") > 0 ? cmd.params["name"] : "rocksdb");
		std::string type(cmd.params.count("type") > 0 ? cmd.params["type"] : "str");

		if (type == "str") {
			std::string ret;
			if (! db->GetProperty(cfhandle, name.c_str(), &ret)) {
				RCM_ERROR("failed to retrieve property \"%s\" from column_family=%s", name.c_str(), cfname.c_str());
				return false;
			}
			RCM_REPORT("Property %s: %s", name.c_str(), ret.c_str());
		} else if (type == "int") {
			uint64_t ret;
			if (! db->GetIntProperty(cfhandle, name.c_str(), &ret)) {
				RCM_ERROR("failed to retrieve property \"%s\" from column_family=%s", name.c_str(), cfname.c_str());
				return false;
			}
			RCM_REPORT("Property %s: %s", name.c_str(), v2s(ret));
		} else if (type == "map") {
			std::map<std::string, std::string> mstats;
			if (! db->GetMapProperty(cfhandle, name.c_str(), &mstats)) {
				RCM_ERROR("failed to retrieve property \"%s\" from column_family=%s", name.c_str(), cfname.c_str());
				return false;
			}
			std::string ret;
			for (const auto& i: mstats) {
				ret += std::string("\t") + i.first + ": " + i.second + "\n";
			}
			RCM_REPORT("Properties:\n%s", ret.c_str());
		} else {
			RCM_ERROR("invalid type \"%s\". Must be str, int, or map.", type.c_str());
			return false;
		}

		return true;
	}

	bool handle_metadata(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		auto cfhandle = get_cfhandle(cmd);
		if (cfhandle == nullptr) return false;
		std::string cfname(cmd.params.count("column_family") > 0 ? cmd.params["column_family"] : "default");

		nlohmann::ordered_json json;

		ROCKSDB_NAMESPACE::ColumnFamilyMetaData metadata;
		db->GetColumnFamilyMetaData(cfhandle, &metadata);
		json["name"] = metadata.name;
		json["size"] = metadata.size;
		json["file_count"] = metadata.file_count;
		json["level_count"] = metadata.levels.size();
		for (auto &l: metadata.levels) {
			std::string level_prefix = alutils::sprintf("L%s.", v2s(l.level));
			json[level_prefix + "size"] = l.size;
			json[level_prefix + "file_count"] = l.files.size();

#			define FileAttrs( _f )                     \
				_f(name, std::string);                 \
				_f(size, std::to_string);              \
				_f(num_reads_sampled, std::to_string); \
				_f(num_entries, std::to_string);       \
				_f(num_deletions, std::to_string);     \
				_f(being_compacted, std::to_string)
#			define f_declare(a_name, ...) \
				std::string file_##a_name
#			define f_append(a_name, convf) \
				file_##a_name += (file_##a_name.length() > 0) ? ", " : ""; \
				file_##a_name += convf(f.a_name)
#			define f_add(a_name, ...) \
				json[level_prefix + "files." #a_name] = file_##a_name

			FileAttrs(f_declare);
			for (auto &f: l.files) {
				FileAttrs(f_append);
			}
			FileAttrs(f_add);

#			undef FileAttrs
#			undef f_declare
#			undef f_append
#			undef f_add
		}

		RCM_REPORT("Column family metadata.json: %s", json.dump().c_str());
		return true;
	}

	bool handle_getoptions(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		auto cfhandle = get_cfhandle(cmd);
		if (cfhandle == nullptr) return false;
		std::string cfname(cmd.params.count("column_family") > 0 ? cmd.params["column_family"] : "default");

		auto options = db->GetOptions(cfhandle);
		OutputLogger logger(&cmd.output);
		options.Dump(&logger);

		return true;
	}

	bool handle_setoptions(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		auto cfhandle = get_cfhandle(cmd);
		if (cfhandle == nullptr) return false;
		std::string cfname(cmd.params.count("column_family") > 0 ? cmd.params["column_family"] : "default");

		std::unordered_map<std::string, std::string> options;
		for (const auto& i : cmd.params) {
			if (i.first != "column_family") {
				options[i.first] = i.second;
			}
		}

		auto s = db->SetOptions(cfhandle, options);
		if (!s.ok()) {
			RCM_ERROR("SetOptions: %s", s.ToString().c_str());
			return false;
		}

		RCM_PRINT("done!");
		return true;
	}

	bool handle_setdboptions(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		std::unordered_map<std::string, std::string> options;
		for (const auto& i : cmd.params) {
			options[i.first] = i.second;
		}

		auto s = db->SetDBOptions(options);
		if (!s.ok()) {
			RCM_ERROR("SetDBOptions: %s", s.ToString().c_str());
			return false;
		}

		RCM_PRINT("done!");
		return true;
	}

	bool handle_compact_level(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		auto cfhandle = get_cfhandle(cmd);
		if (cfhandle == nullptr) return false;
		std::string cfname(cmd.params.count("column_family") > 0 ? cmd.params["column_family"] : "default");

		ROCKSDB_NAMESPACE::ColumnFamilyMetaData metadata;
		db->GetColumnFamilyMetaData(cfhandle, &metadata);

		std::vector<int>::size_type level = 1;
		if (cmd.params.count("level") > 0) {
			std::istringstream auxs(cmd.params["level"]);
			auxs >> level;
		}
		if (level >= metadata.levels.size()) {
			RCM_ERROR("invalid level: %d", level);
			return false;
		}

		std::vector<int>::size_type target_level = level + 1;
		if (cmd.params.count("target_level") > 0) {
			std::istringstream auxs(cmd.params["target_level"]);
			auxs >> target_level;
		}
		if (target_level >= metadata.levels.size()) {
			RCM_ERROR("invalid target_level: %d");
			return false;
		}

		auto &l = metadata.levels[level];
		std::vector<int>::size_type files = 0;
		if (cmd.params.count("files") > 0) {
			std::istringstream auxs(cmd.params["files"]);
			auxs >> files;
		}
		if (files == 0 || files > l.files.size()) {
			files = l.files.size();
		}

		std::vector<std::string> input_file_names;
		std::vector<int>::size_type c = 0;
		for (auto& f: l.files) {
			if (c++ >= files) break;
			input_file_names.push_back(f.name);
		}

		{
			RCM_PRINT("Column Family %s: compacting %s files of %s from level %s to level %s", cfname.c_str(), v2s(files), v2s(l.files.size()), v2s(level), v2s(target_level));
			ROCKSDB_NAMESPACE::CompactionOptions compact_options;
			auto s = db->CompactFiles(compact_options, cfhandle, input_file_names, target_level);
			if (s.ok()) {
				RCM_PRINT("done!");
			} else {
				RCM_ERROR("failed!");
				return false;
			}
		}

		return true;
	}

	bool handle_test(CommandLine& cmd) {
		RCM_DEBUG("start command handler");

		RCM_PRINT("test response: OK!");
		for (const auto &i : cmd.params) {
			RCM_PRINT("\tcmd.params[%s] = %s", i.first.c_str(), i.second.c_str());
		}
		for (const auto &i : cmd.tags_before) {
			RCM_PRINT("\tcmd.tags_before[%s] = %s", i.first.c_str(), i.second.c_str());
		}
		for (const auto &i : cmd.tags) {
			RCM_PRINT("\tcmd.tags[%s] = %s", i.first.c_str(), i.second.c_str());
		}
		for (const auto &i : tags) {
			RCM_PRINT("\ttags[%s] = %s", i.first.c_str(), i.second.c_str());
		}
		return true;
	}
};

} // namespace RCM

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#undef v2s

////////////////////////////////////////////////////////////////////////////////////
#define RCM_OPEN_CMD                                                                                \
	auto s = DBImpl::Open(db_options, dbname, column_families, handles, dbptr,                           \
						!kSeqPerBatch, kBatchPerTxn);                                                    \
	if (s.ok() && *dbptr != nullptr){                                                                    \
	  (*dbptr)->rcm_controller.reset(static_cast<RCM::Controller*>(new RCM::ControllerImpl(*dbptr, handles)));  \
	}                                                                                                    \
	return s
