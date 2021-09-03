// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <deque>
#include <algorithm>
#include <chrono>
#include <type_traits>

#include <filesystem>

#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <alutils/print.h>
#include <alutils/string.h>

using std::string;
using std::vector;
using std::function;
using std::chrono::nanoseconds;
using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::system_clock;
using std::exception;
using std::invalid_argument;
using std::runtime_error;
using fmt::format;

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

#define DEBUG_F spdlog::debug
#define DEBUG_MSG(format, ...) DEBUG_F("[{}] " __CLASS__ "{}(): " format, __LINE__, __func__ , ##__VA_ARGS__)
#define DEBUG_OUT(format, ...) \
	if (loglevel.level <= Log::LOG_DEBUG_OUT) \
		DEBUG_F("[{}] " __CLASS__ "{}(): " format, __LINE__, __func__ , ##__VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////////

struct LogLevel {
	typedef enum {
		LOG_DEBUG_OUT,
		LOG_DEBUG,
		LOG_INFO,
		N_levels
	} levels;
	const char* map_names[N_levels+1] = {
			"output",
			"debug",
			"info", NULL};
	const alutils::log_level_t map_alutils[N_levels] = {
			alutils::LOG_DEBUG_OUT,
			alutils::LOG_DEBUG,
			alutils::LOG_INFO};
	const spdlog::level::level_enum map_spdlog[N_levels] = {
			spdlog::level::debug,
			spdlog::level::debug,
			spdlog::level::info};

	levels level;

	LogLevel();
	void set(const string& name);
};

extern LogLevel loglevel;

enum OutType {
	otDebug, otInfo, otWarn, otError
};

////////////////////////////////////////////////////////////////////////////////////

template <typename T>
T sum(const vector<T>& src) {
	T ret = 0;
	for (auto i: src) {
		ret += i;
	}
	return ret;
}

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Defer::"

struct Defer {
	function<void()> method;
	Defer(function<void()> method_) : method(method_) {}
	~Defer() noexcept(false) {method();}
};


////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Clock::"

struct Clock {
	system_clock::time_point time_init;

	Clock() { reset(); }
	Clock(Clock&) = default;
	Clock& operator= (Clock&) = default;

	void reset() {
		time_init = system_clock::now();
	}
	uint64_t s() {
		auto time_cur = system_clock::now();
		return std::chrono::duration_cast<seconds>(time_cur - time_init).count();
	}
	uint64_t ms() {
		auto time_cur = system_clock::now();
		return std::chrono::duration_cast<milliseconds>(time_cur - time_init).count();
	}
	uint64_t us() {
		auto time_cur = system_clock::now();
		return std::chrono::duration_cast<microseconds>(time_cur - time_init).count();
	}
	uint64_t ns() {
		auto time_cur = system_clock::now();
		return std::chrono::duration_cast<nanoseconds>(time_cur - time_init).count();
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "TimeSync::"

class TimeSync {
	const long int fuzzy = 100;
	long int stats_interval_ms;
	long int stats_interval_ms_half;
	std::chrono::time_point<std::chrono::system_clock> base_time;
	bool have_report = false;
	std::atomic_ullong last_report = 0;

	public:
	TimeSync(long int stats_interval_s) :
		stats_interval_ms(stats_interval_s * 1000),
		stats_interval_ms_half(stats_interval_ms/2)
	{
		base_time = std::chrono::system_clock::now();
	}

	void new_report() {
		last_report = std::chrono::duration_cast<milliseconds>(std::chrono::system_clock::now() - base_time).count();
		have_report = true;
		DEBUG_MSG("new report");
	}

	long int get_time_shift(const char* exp_name=nullptr) { // time difference in milliseconds
		if (! have_report)
			return 0;

		unsigned long long last_rep_copy = last_report;
		unsigned long long now = std::chrono::duration_cast<milliseconds>(std::chrono::system_clock::now() - base_time).count();
		long int delta = now - last_rep_copy;
		if (delta >= 2*stats_interval_ms)
			return 0;
		delta %= stats_interval_ms;
		if (delta <= stats_interval_ms_half)
			delta *= -1;
		else
			delta = stats_interval_ms - delta;
		if (std::abs(delta) > fuzzy) {
			spdlog::info("Task {}, shift report time: {}", (exp_name != nullptr) ? exp_name : "undefined", delta);
			return delta;
		} else {
			return 0;
		}
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "VectorParser::"

template <typename T>
class VectorParser : public vector<T> {
protected:
	const char*  name;
	const char*  delimiter;
	function<bool(T)> check;
	uint32_t* num;
public:
	VectorParser()
		: name(""), delimiter(","), check(nullptr), num(nullptr), vector<T>() {}
	VectorParser(const char* name, const char* delimiter, function<bool(T)> check=nullptr, uint32_t* num=nullptr)
		: name(name), delimiter(delimiter), check(check), num(num), vector<T>() {}

	void configure(const char* name_, const char* delimiter_, function<bool(T)> check_=nullptr, uint32_t* num_=nullptr) {
		name = name_;
		delimiter = delimiter_;
		check = check_;
		num = num_;
	}

	VectorParser<T>& operator=(const string& src) {
		DEBUG_MSG("receiving: {}", src);
		const char* error_msg = "invalid value in the list {}: \"{}\"";

		this->clear();
		if (num != nullptr && *num == 0) return *this;

		auto aux = alutils::split_str(src, delimiter);
		for (auto i: aux) {
			if constexpr (std::is_same<T, string>::value) {
				if (check != nullptr && !check(i))
					throw invalid_argument(format(error_msg, name, i));
				this->push_back( i );
			} else if constexpr (std::is_same<T, uint32_t>::value) {
				this->push_back( alutils::parseUint32(i, true, 0, format(error_msg, name, i).c_str(), check) );
			} else if constexpr (std::is_same<T, uint64_t>::value) {
				this->push_back( alutils::parseUint64(i, true, 0, format(error_msg, name, i).c_str(), check) );
			} else if constexpr (std::is_same<T, double>::value) {
				this->push_back( alutils::parseDouble(i, true, 0, format(error_msg, name, i).c_str(), check) );
			} else {
				throw invalid_argument(format("type not implemented for list {}", name));
			}
		}

		if (num != nullptr) {
			while (*num < this->size()) {
				this->pop_back();
			}
			if (*num > 1 && this->size() > 1 && this->size() < *num)
				throw invalid_argument(format("the list {} must have either one element or {}", name, *num));
			while (*num > this->size()) {
				auto aux2 = this->operator [](0);
				this->push_back(aux2);
			}
		}

		return *this;
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "TmpDir::"

class TmpDir {
	std::filesystem::path base;

public:
	TmpDir();
	~TmpDir();
	std::filesystem::path getContainerDir(const string& container_name);
	std::filesystem::path getFileCopy(const std::filesystem::path& original_file);
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

const char* E2S(int error);
