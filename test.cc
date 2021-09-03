// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <csignal>
#include <errno.h>
#include <spdlog/spdlog.h>

#include <set>
#include <random>
#include <chrono>
#include <regex>
#include <any>

#include "util.h"

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

void signalHandler(int signal) {
	spdlog::warn("received signal {}", signal);
	std::signal(signal, SIG_DFL);
	std::raise(signal);
}


int main(int argc, char** argv) {
	std::signal(SIGTERM, signalHandler);
	std::signal(SIGSEGV, signalHandler);
	std::signal(SIGINT,  signalHandler);
	std::signal(SIGILL,  signalHandler);
	std::signal(SIGABRT, signalHandler);
	std::signal(SIGFPE,  signalHandler);
	spdlog::set_level(spdlog::level::debug);
	DEBUG_MSG("Initiating...");

	DEBUG_MSG("argc = {}", argc);
	for (int i=0; i<argc; i++) {
		DEBUG_MSG("argv[{}] = {}", i, argv[i]);
	}

	try {
		std::vector<std::string> cmd_list;
		for (int i = 1; i < argc; i++) {
			cmd_list.push_back(argv[i]);
		}


	} catch (const std::exception& e) {
		spdlog::error("Exception received: {}", e.what());
		return 1;
	}

	spdlog::info("return 0");
	return 0;
}
