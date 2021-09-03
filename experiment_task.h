// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <alutils/process.h>

#include "util.h"

#include "nlohmann/json.hpp"

using std::string;
using std::runtime_error;
using fmt::format;

extern std::unique_ptr<TmpDir> tmpdir;

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "ExperimentTask::"

const char* stat_format = "Task {}, STATS: {}";
typedef std::function<void(OutType, const std::string&)> command_return_f;
void default_command_return(OutType type, const std::string& msg) {
	switch (type) {
		case otDebug: spdlog::debug(msg); break;
		case otInfo:  spdlog::info(msg);  break;
		case otWarn:  spdlog::warn(msg);  break;
		case otError: spdlog::error(msg); break;
	}
}

class ExperimentTask {
	protected: //------------------------------------------------------------------
	string name = "";
	string container_name;
	Clock* clock = nullptr;
	nlohmann::ordered_json data;
	std::unique_ptr<alutils::ProcessController> process;
	uint64_t warm_period_s;
	bool stop_ = false;

	bool   have_socket = false;
	string socket_name;

	ExperimentTask() {}

	public: //---------------------------------------------------------------------
	ExperimentTask(const string name_, Clock* clock_, uint64_t warm_period_s_=0) : name(name_), clock(clock_), warm_period_s(warm_period_s_) {
		DEBUG_MSG("constructor of task {}", name);
		if (clock == nullptr)
			throw runtime_error("invalid clock");
		data["time"] = "";
	}
	virtual ~ExperimentTask() {
		DEBUG_MSG("destructor of task {}", name);
		stop_ = true;
		process.reset(nullptr);
		DEBUG_MSG("destructor finished");
	}
	bool isActive(bool throwexcept = true) {
		return (process.get() != nullptr && process->isActive(throwexcept));
	}
	void stop() {
		stop_ = true;
		process.reset(nullptr);
	}

	void print() {
		print(data);
	}

	const string get_name() {
		return name;
	}

	void send_command(const string& cmd, command_return_f return_function) {
		if (!have_socket) {
			return_function(otError, "experiment does not implent socket or it is not active");
		}
		if (stop_) {
			return_function(otError, "not active");
			return;
		}

		try {
			auto socket_path = (tmpdir->getContainerDir(container_name) / socket_name);
			spdlog::info("initiating socket client: {}", socket_path.string());

			alutils::Socket socket_client(
					alutils::Socket::tClient,
					socket_path.string(),
					[return_function](alutils::Socket::HandlerData* data)->void{ return_function(otInfo, data->msg);},
					alutils::Socket::Params{.buffer_size=4096}
			);
			socket_client.send_msg(cmd);

			for (int i=0; i<10; i++) {
				if (stop_ || !socket_client.isActive()) break;
				std::this_thread::sleep_for(std::chrono::milliseconds(200));
			}
			spdlog::info("socket client closed: {}", socket_path.string());
		} catch (std::exception &e) {
			return_function(otError, format("exception received: {}", e.what()));
		}
	}

	void print(nlohmann::ordered_json& j) {
		if (j.size() == 0)
			spdlog::warn("no data in task {}", name);
		auto clock_s = clock->s();
		if (clock_s > warm_period_s) {
			j["time"] = format("{}", clock_s - warm_period_s);
			spdlog::info(stat_format, name, j.dump());
		}
		j.clear();
		j["time"] = "";
	}

	nlohmann::ordered_json get_data_and_clear() {
		nlohmann::ordered_json j = data;
		data.clear();
		data["time"] = "";
		return j;
	}

	void default_stderr_handler (const char* buffer) {
		spdlog::warn("Task {}, stderr: {}", name, buffer);
	}
};
