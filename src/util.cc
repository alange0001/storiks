// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "util.h"

#include <alutils/string.h>
#include <alutils/process.h>

#include <cstdlib>
#include <stdexcept>
#include <regex>
#include <limits>
#include <atomic>
#include <system_error>
#include <fstream>

using std::string;
using std::vector;
using std::function;
using std::exception;
using std::invalid_argument;
using std::regex_search;
using std::regex;
using std::runtime_error;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::system_clock;
using fmt::format;

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

using alutils::vsprintf;
ALUTILS_PRINT_WRAPPER(alutils_print_debug,    spdlog::debug("{}", msg));
ALUTILS_PRINT_WRAPPER(alutils_print_info,     spdlog::info("{}", msg));
ALUTILS_PRINT_WRAPPER(alutils_print_warn,     spdlog::warn("{}", msg));
ALUTILS_PRINT_WRAPPER(alutils_print_error,    spdlog::error("{}", msg));
ALUTILS_PRINT_WRAPPER(alutils_print_critical, spdlog::critical("{}", msg));

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "LogLevel::"

LogLevel loglevel;

LogLevel::LogLevel() {
	alutils::print_debug_out = alutils_print_debug;
	alutils::print_debug     = alutils_print_debug;
	alutils::print_info      = alutils_print_info;
	alutils::print_notice    = alutils_print_info;
	alutils::print_warn      = alutils_print_warn;
	alutils::print_error     = alutils_print_error;
	alutils::print_critical  = alutils_print_critical;

	set(map_names[LOG_INFO]);
}

void LogLevel::set(const string& name) {
	for (int i=0; map_names[i] != NULL; i++) {
		if (name == map_names[i]) {
			level = (levels)i;
			alutils::log_level = map_alutils[i];
			spdlog::set_level(map_spdlog[i]);
			DEBUG_MSG("set log level to {}", name);
			return;
		}
	}
	string aux = format("invalid log level: {}. Possible values: {}", name, map_names[0]);
	for (int i=1; map_names[i] != NULL; i++)
		aux += string(", ") + map_names[i];
	throw invalid_argument(aux);
}


////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "CommunicationDir::"

CommunicationDir::CommunicationDir() {
	auto envbase = getenv("STORIKS_COMMUNICATION_DIR");
	if (envbase != nullptr) {
		std::filesystem::path aux(envbase);
		if (std::filesystem::is_directory(aux)) {
			path = aux;
			active = true;
			savePID();
		} else {
			throw std::runtime_error(format("invalid communication directory: {}", aux.c_str()).c_str());
		}
	}
}

void CommunicationDir::savePID() {
	const char* filename = "storiks.pid";
	auto filepath = path / filename;

	if (std::filesystem::exists(filepath)) {
		spdlog::warn("there is a file named \"{}\"", filepath.c_str());
		if (!std::filesystem::is_regular_file(filepath))
			throw std::runtime_error(format("invalid existent file \"{}\"", filepath.c_str()).c_str());

		int pid;
		std::ifstream stream(filepath.c_str());
		if (stream.fail())
			throw std::runtime_error(format("failed to open file \"{}\"", filepath.c_str()).c_str());
		stream >> pid;
		if (!stream.fail()) {
			spdlog::warn("checking the existence of a process with PID = {}", pid);
			if (std::filesystem::exists(format("/proc/{}", pid).c_str())) {
				std::ifstream stream_stat(format("/proc/{}/status", pid).c_str());
				if (!stream_stat.fail()) {
					std::string aux;
					std::smatch sm;
					while (std::getline(stream_stat, aux)) {
						std::regex_search(aux, sm, std::regex("^State:\\s+([\\w])\\s+(.*)"));
						if (sm.size() > 1) {
							spdlog::info("process with PID = {} has state {} {}", pid, sm.str(1), sm.str(2));
							if (sm.str(1) != "Z")
								throw std::runtime_error(format("there is another instance of storiks running with PID={}", pid).c_str());

						}
					}
				}
			}
		}
	}
	writeStr(filename, format("{}\n", getpid()), {.overwrite=true, .throw_except=true});
}

bool CommunicationDir::isActive() {
	return active;
}

std::filesystem::path CommunicationDir::getPath() {
	return path;
}

std::pair<bool, string> CommunicationDir::writeStr(const std::filesystem::path& filename, const string& str, const CommunicationDir::WriteOptions& options) {
	try {
		if (! active)
			throw std::runtime_error("communication directory is not active");

		auto filepath = path / filename;
		if (std::filesystem::exists(filepath) && !options.overwrite)
			throw std::runtime_error(format("overwrite file \"{}\" is not allowed", filepath.c_str()).c_str());

		std::ofstream stream(filepath.c_str(), std::ios::trunc);
		if (stream.fail())
			throw std::runtime_error(format("failed to create file \"{}\"", filepath.c_str()).c_str());
		stream << str;
		if (stream.fail())
			throw std::runtime_error(format("failed to write file \"{}\"", filepath.c_str()).c_str());

		return {true, ""};

	} catch (const std::exception& e) {
		if (options.throw_except)
			throw e;
		else {
			if (options.print_error)
				spdlog::error("{}", e.what());
			return {false, e.what()};
		}
	}
}


////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "TmpDir::"

static int tmpdir_runcount = 1;
static std::atomic<int> tmpdir_filecount = 1;

TmpDir::TmpDir(CommunicationDir* commdir) {
	DEBUG_MSG("constructor");


	auto prebase = (commdir != nullptr && commdir->isActive()) ? commdir->getPath() : std::filesystem::temp_directory_path();
	DEBUG_MSG("prebase = {}", prebase.c_str());
	if (! std::filesystem::is_directory(prebase)) {
		throw runtime_error(format("invalid base temporary directory: {}", prebase.c_str()).c_str());
	}

	while (true) {
		base = prebase / ( string("run-") + std::to_string(tmpdir_runcount++) );
		DEBUG_MSG("base = {}", base.c_str());

		if ( std::filesystem::create_directories(base) ) {
			spdlog::info("experiment temporary directory: {}", base.c_str());
			break;
		} else {
			if (!std::filesystem::exists(base))
				throw runtime_error(format("impossible to create the experiment temporary directory: {}", prebase.c_str()).c_str());
			spdlog::warn("failed to create the experiment temporary directory {}. Try again.", base.c_str());
		}

		if (tmpdir_runcount > 1024) {
			throw runtime_error(format("failed to create the experiment temporary directory: {}", prebase.c_str()).c_str());
		}
	}

	DEBUG_MSG("constructor finished");
}

TmpDir::~TmpDir() {
	DEBUG_MSG("destructor");
	if (! std::filesystem::remove_all(base) ) {
		spdlog::error("failed to delete experiment temporary directory \"{}\"", base.c_str());
	}
}

std::filesystem::path TmpDir::getContainerDir(const string& container_name) {
	std::filesystem::path ret = base / container_name;
	std::error_code ec;
	if ( std::filesystem::is_directory(ret, ec) ) {
		DEBUG_MSG("temporary container directory already exists: {}", ret.c_str());
		return ret;
	}
	DEBUG_MSG("creating temporary container directory: {}", ret.c_str());
	if (! std::filesystem::create_directories(ret, ec) ){
		throw runtime_error(format("failed to create temporary directory \"{}\": {}", ret.c_str(), ec.message()).c_str());
	}
	return ret;
}

std::filesystem::path TmpDir::getFileCopy(const std::filesystem::path& original_file) {
	std::error_code ec;
	if (! std::filesystem::is_regular_file(original_file) ) {
		throw runtime_error(format("file \"{}\" is not a regular file", original_file.c_str()).c_str());
	}
	string tmpfile = original_file.filename().string() + std::to_string(tmpdir_filecount++);
	std::filesystem::path ret = base / tmpfile;
	DEBUG_MSG("creating temporary file copy: {}", ret.c_str());
	if (! std::filesystem::copy_file(original_file, ret, ec) ) {
		throw runtime_error(format("failed to copy file \"{}\" to \"{}\": {}", original_file.c_str(), ret.c_str(), ec.message()).c_str());
	}
	return ret;
}

std::filesystem::path TmpDir::getBase() {
	return base;
}


////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

const char* E2S(int error) {
#	define return_error(val) if (error == -val) return #val
	return_error(EAGAIN );
	return_error(EPERM  );
	return_error(ENOENT );
	return_error(ESRCH  );
	return_error(EINTR  );
	return_error(EIO    );
	return_error(ENXIO  );
	return_error(E2BIG  );
	return_error(ENOEXEC);
	return_error(EBADF  );
	return_error(ECHILD );
	return_error(EAGAIN );
	return_error(ENOMEM );
	return_error(EACCES );
	return_error(EFAULT );
	return_error(ENOTBLK);
	return_error(EBUSY  );
	return_error(EEXIST	);
	return_error(EXDEV  );
	return_error(ENODEV	);
	return_error(ENOTDIR);
	return_error(EISDIR );
	return_error(EINVAL );
	return_error(ENFILE );
	return_error(EMFILE );
	return_error(ENOTTY );
	return_error(ETXTBSY);
	return_error(EFBIG  );
	return_error(ENOSPC );
	return_error(ESPIPE );
	return_error(EROFS  );
	return_error(EMLINK );
	return_error(EPIPE  );
	return_error(EDOM   );
	return_error(ERANGE );
	return "unknown";
#   undef return_error
}
