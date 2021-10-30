// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <csignal>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <libaio.h>

#include <cstdlib>
#include <random>
#include <chrono>
#include <thread>
#include <functional>
#include <random>

#include <gflags/gflags.h>

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

#define ERROR_MSG(format, ...) fprintf(stderr, "ERROR: " format "\n", ##__VA_ARGS__)
#define WARN_MSG(format, ...) fprintf(stderr, "WARN: " format "\n", ##__VA_ARGS__)
#define INFO_MSG(format, ...) fprintf(stderr, "INFO: " format "\n", ##__VA_ARGS__)
#define DEBUG 1
#ifdef DEBUG
#define DEBUG_MSG(format, ...) fprintf(stderr, "DEBUG [%d] %s(): " format "\n", __LINE__, __func__ , ##__VA_ARGS__)
#else
#define DEBUG_MSG(format, ...)
#endif

#define V2S(val) std::to_string(val).c_str()

////////////////////////////////////////////////////////////////////////////////////

DEFINE_int32(iodepth, 4, "I/O depth");
DEFINE_int32(block_size, 4, "block size");
DEFINE_int32(duration, 20, "duration");
DEFINE_string(filename, "0", "file name");

////////////////////////////////////////////////////////////////////////////////////

static bool stop = false;

static void signalHandler(int signal) {
	WARN_MSG("received signal %d", signal);
	stop = true;
	std::signal(signal, SIG_DFL);
}

////////////////////////////////////////////////////////////////////////////////////

struct Defer {
	std::function<void()> method;
	Defer(std::function<void()> method_) : method(method_) {}
	~Defer() noexcept(false) {method();}
};

std::random_device  rd;
std::mt19937_64     rand_eng;
std::uniform_int_distribution<uint64_t> dist;

static void rand_init() {
	rand_eng.seed(rd());
}

static void randomize_buffer(char* buffer, unsigned size) {
	auto buffer64 = (uint64_t*) buffer;
	unsigned size64 = size / (sizeof(uint64_t)/sizeof(char));

	for (unsigned i = 0; i < size64; i++) {
		buffer64[i] = dist(rand_eng);
	}
}

static void randomize_buffer2(char* buffer, unsigned size) {
	auto buffer64 = (uint64_t*) buffer;
	unsigned size64 = size / (sizeof(uint64_t)/sizeof(char));

	unsigned cycle = 4 * (size / 512);
	if (cycle < 10)
		cycle = 10;

	while (!stop) {
		for (unsigned i = 0; i < 20; i++) {
			auto r = dist(rand_eng);
			buffer64[r % size64] = r;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}
}

////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
	std::signal(SIGTERM, signalHandler);
	std::signal(SIGINT,  signalHandler);
	INFO_MSG("Initiating...");

	std::vector<std::string> cmd_list;
	INFO_MSG("argc = %d", argc);
	for (typeof(argc) i=0; i<argc; i++) {
		INFO_MSG("argv[%d] = %s", i, argv[i]);
		cmd_list.push_back(argv[i]);
	}
	gflags::SetUsageMessage(std::string("\nUSAGE:\n\t") + argv[0] + " [OPTIONS]...");
	gflags::ParseCommandLineFlags(&argc, &argv, true);

	try {
		rand_init();

		// -----------------------------------------------------------
		// PARAMETERS:
		uint32_t duration     = FLAGS_duration;
		std::string filename  = FLAGS_filename;
		uint32_t iodepth      = FLAGS_iodepth;
		uint32_t block_size   = FLAGS_block_size;
		uint32_t block_size_b = block_size * 1024;

		// -----------------------------------------------------------
		// FILE INFORMATION:
		uint64_t file_blocks;
		{
			DEBUG_MSG("get stats from file: %s", filename.c_str());
			struct stat st;
			if (stat(filename.c_str(), &st) == EOF)
				throw std::runtime_error("can't read file stats");
			file_blocks = st.st_size / block_size_b;
			DEBUG_MSG("\tst_size = %s", V2S(st.st_size));
			DEBUG_MSG("\tst_blksize = %s", V2S(st.st_blksize));
		}
		DEBUG_MSG("file_blocks = %s", V2S(file_blocks));

		std::uniform_int_distribution<uint64_t> rand_block(0, file_blocks);

		// -----------------------------------------------------------
		// OPEN FILE:
		DEBUG_MSG("open file \"%s\"", filename.c_str());
		auto filed = open(filename.c_str(), O_RDWR|O_DIRECT, 0640);
		if (filed < 0) {
			throw std::runtime_error("can't open file");
		}
		Defer def_close([&]{close(filed);});

		// -----------------------------------------------------------
		// LIBAIO CONTEXT AND DATA:
		io_context_t ctx;
		memset(&ctx, 0, sizeof(ctx));
		io_queue_init(iodepth, &ctx);

		iocb iocb_data[iodepth];
		char* buffers = (char*) std::aligned_alloc(block_size_b, iodepth * block_size_b);
		randomize_buffer(buffers, iodepth * block_size_b);

		// -----------------------------------------------------------
		// COUNTDOWN THREAD:
		std::thread t1([&]{
			std::this_thread::sleep_for(std::chrono::seconds(duration));
			stop = true;
		});
		t1.detach();

		// RANDOMIZE BUFFER THREAD:
		std::thread t2([&]{
			randomize_buffer2(buffers, iodepth * block_size_b);
		});
		Defer def_t2([&]{ t2.join(); });

		// ===========================================================
		// 1st I/O SUBMIT:
		iocb* iocbs[1];
		int rc;
		io_event events[1];
		for (typeof(iodepth) i = 0; i < iodepth; i++) {
			io_prep_pwrite(&iocb_data[i], filed, &buffers[i * block_size_b], block_size_b, rand_block(rand_eng) * block_size_b);
			iocbs[0] = &iocb_data[i];
			rc = io_submit(ctx, 1, iocbs);
			if (rc != 1)
				ERROR_MSG("io_submit returned %s", V2S(rc));
		}
		// ===========================================================
		// MAIN LOOP:
		while (!stop) {
			rc = io_getevents(ctx, 1, 1, events, nullptr);
			if (rc > 0) {
				if (events->res != block_size_b) {
					ERROR_MSG("res=%s != block_size_b", V2S(events->res));
				}
				io_prep_pwrite(events->obj, filed, events->obj->u.c.buf, block_size_b, rand_block(rand_eng) * block_size_b);
				iocbs[0] = events->obj;
				rc = io_submit(ctx, 1, iocbs);
				if (rc != 1)
					ERROR_MSG("io_submit returned %s", V2S(rc));
			} else {
				ERROR_MSG("io_getevents returned %s", V2S(rc));
			}
		}
		io_queue_release(ctx);
		// ===========================================================

	} catch (const std::exception& e) {
		ERROR_MSG("Exception received: %s", e.what());
		return 1;
	}

	INFO_MSG("return 0");
	return 0;
}
