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
#define __CLASS__ "TmpDir::"

static std::atomic<int> tmpdir_filecount = 1;

TmpDir::TmpDir() {
	DEBUG_MSG("constructor");
	base = std::filesystem::temp_directory_path() / ( string("rocksdb_test-") + std::to_string(getpid()) );
	DEBUG_MSG("base directory: {}", base.c_str());
	if (! std::filesystem::create_directories(base) ){
		throw runtime_error(format("failed to create temporary directory: {}", base.c_str()).c_str());
	}
	DEBUG_MSG("constructor finished");
}

TmpDir::~TmpDir() {
	DEBUG_MSG("destructor");
	if (! std::filesystem::remove_all(base) ) {
		spdlog::error("failed to delete temorary directory \"{}\"", base.c_str());
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
