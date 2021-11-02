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
#include <spdlog/spdlog.h>

#include "bitmap.h"

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

////////////////////////////////////////////////////////////////////////////////////

DEFINE_int32(iodepth, 4, "I/O depth");
DEFINE_int32(block_size, 4, "block size");
DEFINE_int32(duration, 20, "duration");
DEFINE_string(filename, "0", "file name");

////////////////////////////////////////////////////////////////////////////////////

static bool stop = false;

static void signalHandler(int signal) {
	spdlog::warn("received signal {}", signal);
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
std::uniform_int_distribution<uint64_t> unirand64;

static void rand_init() {
	rand_eng.seed(rd());
}

static void randomize_buffer(char* buffer, unsigned size) {
	auto buffer64 = (uint64_t*) buffer;
	unsigned size64 = size / (sizeof(uint64_t)/sizeof(char));

	for (unsigned i = 0; i < size64; i++) {
		buffer64[i] = unirand64(rand_eng);
	}
}

static void randomize_buffer2(char* buffer, unsigned size) {
	auto buffer64 = (uint64_t*) buffer;

	typeof(size) size64 = size / (sizeof(uint64_t)/sizeof(char));
	typeof(size) step = 64;

	while (!stop) {
		typeof(size) i0 = unirand64(rand_eng) % step;
		for (typeof(size) i = i0; i < size64; i+=step) {
			buffer64[i] = unirand64(rand_eng);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
}

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

int main(int argc, char** argv) {
	std::signal(SIGTERM, signalHandler);
	std::signal(SIGINT,  signalHandler);
	spdlog::set_level(spdlog::level::debug);
	spdlog::info("Initiating...");

	std::vector<std::string> cmd_list;
	spdlog::info("argc = {}", argc);
	for (typeof(argc) i=0; i<argc; i++) {
		spdlog::info("argv[{}] = {}", i, argv[i]);
		cmd_list.push_back(argv[i]);
	}
	gflags::SetUsageMessage(std::string("\nUSAGE:\n\t") + argv[0] + " [OPTIONS]...");
	gflags::ParseCommandLineFlags(&argc, &argv, true);

	try {
		rand_init();

		// -----------------------------------------------------------
		// PARAMETERS:
		uint32_t duration     = FLAGS_duration;     spdlog::info("duration   = {}", duration);
		std::string filename  = FLAGS_filename;     spdlog::info("filename   = {}", filename.c_str());
		uint32_t iodepth      = FLAGS_iodepth;      spdlog::info("iodepth    = {}", iodepth);
		uint32_t block_size   = FLAGS_block_size;   spdlog::info("block_size = {}", block_size);
		uint32_t block_size_b = block_size * 1024;

		// -----------------------------------------------------------
		// FILE INFORMATION:
		uint64_t file_blocks;
		{
			DEBUG_MSG("get stats from file: {}", filename);
			struct stat st;
			if (stat(filename.c_str(), &st) == EOF)
				throw std::runtime_error("can't read file stats");
			file_blocks = st.st_size / block_size_b;
			DEBUG_MSG("\tst_size = {}", st.st_size);
			DEBUG_MSG("\tst_blksize = {}", st.st_blksize);
		}
		DEBUG_MSG("file_blocks = {}", file_blocks);
		Bitmap used_bitmap(file_blocks);

		std::uniform_int_distribution<uint64_t> rand_block(0, file_blocks -1);

		// -----------------------------------------------------------
		// OPEN FILE:
		DEBUG_MSG("open file \"{}\"", filename);
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
		std::unique_ptr<std::unique_ptr<char>[]> buffers_mem(new std::unique_ptr<char>[iodepth]);
		for (int i=0; i<iodepth; i++) {
			buffers_mem[i].reset(static_cast<char*>(std::aligned_alloc(block_size_b * 8, block_size_b)));
			randomize_buffer(buffers_mem[i].get(), block_size_b);
		}

		// -----------------------------------------------------------
		// COUNTDOWN THREAD:
		std::thread t1([&]{
			std::this_thread::sleep_for(std::chrono::seconds(duration));
			stop = true;
		});
		t1.detach();

		// RANDOMIZE BUFFER THREAD:
		std::thread t2([&]{
			for (int i=0; i<iodepth; i++) {
				randomize_buffer2(buffers_mem[i].get(), block_size_b);
			}
		});
		Defer def_t2([&]{ t2.join(); });

		// ===========================================================
		// 1st I/O SUBMIT:
		iocb* iocbs[1];
		int rc;
		io_event events[1];
		for (typeof(iodepth) i = 0; i < iodepth; i++) {
			uint64_t next_block = used_bitmap.next_unused( rand_block(rand_eng) );
			io_prep_pwrite(&iocb_data[i], filed, buffers_mem[i].get(), block_size_b, next_block * block_size_b);
			iocbs[0] = &iocb_data[i];
			rc = io_submit(ctx, 1, iocbs);
			if (rc != 1)
				spdlog::error("io_submit returned {}", rc);
		}
		// ===========================================================
		// MAIN LOOP:
		while (!stop) {
			rc = io_getevents(ctx, 1, 1, events, nullptr);
			if (rc > 0) {
				if (events->res != block_size_b) {
					spdlog::error("res={} != block_size_b", events->res);
				}
				uint64_t next_block = used_bitmap.next_unused( rand_block(rand_eng) );
				io_prep_pwrite(events->obj, filed, events->obj->u.c.buf, block_size_b, next_block * block_size_b);
				iocbs[0] = events->obj;
				rc = io_submit(ctx, 1, iocbs);
				if (rc != 1)
					spdlog::error("io_submit returned {}", rc);
			} else {
				spdlog::error("io_getevents returned {}", rc);
			}
		}
		io_queue_release(ctx);
		// ===========================================================

	} catch (const std::exception& e) {
		spdlog::error("Exception received: {}", e.what());
		return 1;
	}

	spdlog::info("return 0");
	return 0;
}
