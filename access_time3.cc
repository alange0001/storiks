// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "version.h"

#include <string>
#include <thread>
#include <stdexcept>
#include <functional>
#include <random>
#include <regex>
#include <limits>
#include <set>

#include <iostream>

#include <csignal>
#include <cstdio>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <libaio.h>

#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <alutils/io.h>
#include <alutils/process.h>
#include <alutils/socket.h>

#include "access_time3_args.h"
#include "util.h"

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""

const size_t aligned_buffer_size = 512;
struct alignas(aligned_buffer_size) aligned_buffer_t {
	char data[aligned_buffer_size];
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Randomizer::"

class Randomizer {
	public:
	std::random_device  rd;
	std::mt19937        rand_eng;
	std::mt19937_64     rand_eng64;
	const uint32_t      ratio_precision = 1024;
	std::unique_ptr<std::uniform_int_distribution<uint32_t>> dist_ratio;

	Randomizer() {
		auto seed = rd();
		rand_eng.seed(seed);
		rand_eng64.seed(seed);
		dist_ratio.reset(new std::uniform_int_distribution<uint32_t>(0, ratio_precision -1));
	}

	bool randomize_ratio(double ratio) {
		return ((*dist_ratio)(rand_eng) < static_cast<uint32_t>(ratio * ratio_precision));
	}

	void randomize_buffer(char* buffer, uint64_t size, uint64_t step=1) {
		assert(buffer != nullptr);
		assert(size > 0);
		assert(step > 0);

		const uint64_t size_ratio = sizeof(uint64_t) / sizeof(char);
		uint64_t size_type = size / size_ratio;
		std::uniform_int_distribution<uint64_t> dist;

		uint64_t first_i = 0;
		if (step > 1) {
			std::uniform_int_distribution<uint64_t> dist_step(0, step-1);
			first_i = dist_step(rand_eng64);
		}

		uint64_t* b = reinterpret_cast<uint64_t*>(buffer);
		for (uint64_t i = first_i; i < size_type; i += step) {
			b[i] = dist(rand_eng64);
		}
	}

} randomizer;


////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Stats::"

struct Stats {
	uint64_t blocks        = 0;
	uint64_t blocks_read   = 0;
	uint64_t blocks_write  = 0;
	uint64_t KB_read  = 0;
	uint64_t KB_write = 0;
	Stats operator- (const Stats& val) {
		Stats ret = *this;
		ret.blocks       -= val.blocks;
		ret.blocks_read  -= val.blocks_read;
		ret.blocks_write -= val.blocks_write;
		ret.KB_read      -= val.KB_read;
		ret.KB_write     -= val.KB_write;
		return ret;
	}
	Stats& operator+= (const Stats& val) {
		blocks       += val.blocks;
		blocks_read  += val.blocks_read;
		blocks_write += val.blocks_write;
		KB_read      += val.KB_read;
		KB_write     += val.KB_write;
		return *this;
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "GenericEngine::"

typedef std::function<void(const Stats& val)> increment_stats_t;

class GenericEngine {
	public: //---------------------------------------------------------------------
	GenericEngine() {}
	virtual ~GenericEngine() {}
	virtual void make_requests(bool& stop_) {}
	virtual void wait() {}
	virtual bool is_multithread() {return false;}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "AccessParams::"

struct AccessParams {
	typeof(Args::block_size) block_size;
	size_t     size;
	long long  offset;
	bool       write;
	bool       dsync;
};

typedef std::function<AccessParams()> access_params_t;
typedef std::function<void(long long offset)> offset_released_t;

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "PosixEngine::"

class PosixEngine : public GenericEngine {
	int fd;

	increment_stats_t increment_stats;
	access_params_t access_params;
	offset_released_t offset_released;

	std::unique_ptr<aligned_buffer_t[]> buffer_mem;
	char*      buffer = nullptr;
	size_t     cur_size = 0;
	long long  cur_offset = 0;
	bool       cur_write = false;

	public:  // ------------------------------------------------------------
	PosixEngine(int fd_, increment_stats_t increment_stats_,
	          access_params_t access_params_, offset_released_t offset_released_)
	          : fd(fd_), increment_stats(increment_stats_),
	            access_params(access_params_), offset_released(offset_released_)
	{
		DEBUG_MSG("constructor");
	}

	~PosixEngine() {
		DEBUG_MSG("destructor");
	}

	void make_requests(bool& stop_) {
		if (stop_) return;

		auto params = access_params();
		assert(params.size > 0);
		if (cur_size != params.size) {
			DEBUG_MSG("request size changed from {} to {}", cur_size, params.size);
			cur_size = params.size;
			buffer_mem.reset(new aligned_buffer_t[cur_size/sizeof(aligned_buffer_t)]);
			buffer = buffer_mem[0].data;
			randomizer.randomize_buffer(buffer, cur_size);
		} else if (params.write && cur_write) { // randomize 5% of the buffer due to repeated writes
			randomizer.randomize_buffer(buffer, cur_size, 20);
		}

		auto stats = Stats{
			.blocks = 1,
			.blocks_read  = static_cast<uint64_t>( (!params.write) ? 1 : 0 ),
			.blocks_write = static_cast<uint64_t>( ( params.write) ? 1 : 0 ),
			.KB_read  = (!params.write) ? params.block_size : 0,
			.KB_write = ( params.write) ? params.block_size : 0,
		};

		if (cur_offset + cur_size != params.offset) {
			if (lseek(fd, params.offset, SEEK_SET) == -1)
				throw std::runtime_error(fmt::format("seek error: {}", strerror(errno)).c_str());
		}
		cur_offset = params.offset;
		cur_write = params.write;

		if (stop_) return;

		if (params.write) {
			if (write(fd, buffer, cur_size) == -1)
				throw std::runtime_error(fmt::format("write error: {}", strerror(errno)).c_str());
		} else {
			if (read(fd, buffer, cur_size) == -1)
				throw std::runtime_error(fmt::format("read error: {}", strerror(errno)).c_str());
		}

		offset_released(cur_offset);
		increment_stats(stats);
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "AIORequest::"

class AIORequest {
	public:  // ------------------------------------------------------------
	struct Options {
		int                 pos_count = 0;
		int                 fd;
		io_context_t*       ctx;
		access_params_t     access_params;
		offset_released_t   offset_released;

		Options(int fd_, io_context_t* ctx_, access_params_t access_params_,
		        offset_released_t offset_released_)
		        : fd(fd_), ctx(ctx_),
				  access_params(access_params_),
				  offset_released(offset_released_) {}
	};

	Options*         options;
	int              pos    = -1;
	bool             active = false;
	bool             write  = false;
	iocb             cb;
	Stats            stats;
	size_t           size   = 0;
	long long        offset = 0;

	std::unique_ptr<aligned_buffer_t[]> buffer_mem;
	char*            buffer = nullptr;

	AIORequest(Options* options_) : options(options_) {
		assert( options != nullptr );
		pos = options->pos_count++;
	}

	~AIORequest() {
		if (active) {
			spdlog::info("AIORequest[{}] is still active. Canceling it.", pos);
			io_event event;
			auto ret = io_cancel(*(options->ctx), &cb, &event);
			if (ret < 0) {
				spdlog::warn("\tio_cancel returned error {}:{}", ret, E2S(ret));
			}
		}
	}

	bool request() {
		assert(pos >= 0);
		assert(!active);

		auto params = options->access_params();
		assert(params.size > 0);
		if (size != params.size) {
			DEBUG_MSG("request size changed from {} to {}", size, params.size);
			size = params.size;
			buffer_mem.reset(new aligned_buffer_t[size/sizeof(aligned_buffer_t)]);
			buffer = buffer_mem[0].data;
			randomizer.randomize_buffer(buffer, size);
		} else if (params.write && write) { // randomize 5% of the buffer due to repeated writes
			randomizer.randomize_buffer(buffer, size, 20);
		}

		stats = Stats{
			.blocks = 1,
			.blocks_read  = static_cast<uint64_t>( (!params.write) ? 1 : 0 ),
			.blocks_write = static_cast<uint64_t>( ( params.write) ? 1 : 0 ),
			.KB_read  = (!params.write) ? params.block_size : 0,
			.KB_write = ( params.write) ? params.block_size : 0,
		};

		write = params.write;
		offset = params.offset;

		if (params.write) {
			io_prep_pwrite(&cb, options->fd, buffer, size, offset);
			if (params.dsync) {
				cb.aio_rw_flags |= RWF_DSYNC;
			}
		} else { //read
			io_prep_pread(&cb, options->fd, buffer, size, offset);
		}
		cb.data = this;

		iocb* iocbs[] = {&cb};
		auto ret = io_submit(*(options->ctx), 1, iocbs);
		if (ret == 1) {
			active = true;
			return true;
		} else if (ret == 0) {
			spdlog::warn("aio submit returned 0");
		} else if (ret == -EINTR || ret == -EAGAIN) {
			spdlog::warn("aio submit returned {}:{}", ret, E2S(ret));
		} else {
			throw std::runtime_error(fmt::format("failed to submit the aio request: {}:{}", ret, E2S(ret)).c_str());
		}
		return false;
	}

	void request_finished() {
		assert(active);
		active = false;
		options->offset_released(offset);
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "AIOEngine::"

class AIOEngine : public GenericEngine {
	io_context_t ctx;

	std::unique_ptr<AIORequest::Options> request_options;
	std::unique_ptr<std::unique_ptr<AIORequest>[]> request_list;

	uint32_t& iodepth;
	increment_stats_t increment_stats;

	public:  // ------------------------------------------------------------
	AIOEngine(int fd, uint32_t& iodepth_, increment_stats_t increment_stats_,
	          access_params_t access_params_, offset_released_t offset_released_)
	          : iodepth(iodepth_), increment_stats(increment_stats_)
	{
		DEBUG_MSG("constructor");

		memset(&ctx, 0, sizeof(ctx));
		auto ret = io_setup(max_iodepth, &ctx);
		if (ret != 0) {
			throw std::runtime_error(fmt::format("io_setup returned error {}:{}", ret, E2S(ret)).c_str());
		}

		request_options.reset(new AIORequest::Options(fd, &ctx, access_params_, offset_released_));

		request_list.reset(new std::unique_ptr<AIORequest>[max_iodepth]);
		for (int i = 0; i < max_iodepth; i++) {
			request_list[i].reset(new AIORequest(request_options.get()));
		}
	}

	~AIOEngine() {
		DEBUG_MSG("destructor");

		spdlog::info("waiting for pending requests");
		timespec timeout = {.tv_sec  = 0, .tv_nsec = 300 * 1000 * 1000 };
		io_event events[max_iodepth];
		auto ret = io_getevents(ctx, iodepth, max_iodepth, events, &timeout);
		if (ret < 0) {
			spdlog::error("io_getevents returned error {}:{}", ret, E2S(ret));
		}
		for (int i = 0; i < ret; i++)
			request_list[i]->active = false;

		DEBUG_MSG("removing request_list");
		request_list.reset(nullptr);

		DEBUG_MSG("io_destroy(ctx)");
		ret = io_destroy(ctx);
		if (ret < 0) {
			spdlog::error("io_destroy returned error {}:{}", ret, E2S(ret));
		}
	}

	void make_requests(bool& stop_) {
		for (int i = 0; i < iodepth; i++ ){
			if (! request_list[i]->active) {
				request_list[i]->request();
			}
		}

		if (stop_) return;

		timespec timeout = {.tv_sec  = 0, .tv_nsec = 200 * 1000 * 1000 };
		io_event events[max_iodepth];

		auto nevents = io_getevents(ctx, 1, max_iodepth, events, &timeout);

		if (stop_) return;

		if (nevents < 0) {
			if (nevents != -EAGAIN && nevents != -EINTR) {
				spdlog::warn("io_getevents returned {}:{}", nevents, E2S(nevents));
			} else {
				throw std::runtime_error(fmt::format("io_getevents returned error: {}:{}", nevents, E2S(nevents)).c_str());
			}
		} else if (nevents > 0) {
			Stats stats_sum;
			for (int i = 0; i < nevents; i++) {
				if (events[i].data) {
					auto req = ((AIORequest*) events[i].data);
					assert(req->pos >= 0 && req->pos < max_iodepth);
					req->request_finished();
					stats_sum += req->stats;

					if (req->pos < iodepth)
						req->request();
				}
			}
			increment_stats(stats_sum);
		}
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Prwv2Engine::"

class Prwv2Engine : public GenericEngine {
	bool wait_ = true;
	bool stop = false;

	std::unique_ptr<std::unique_ptr<std::thread>[]> threads;
	std::exception_ptr thread_exception;

	int       fd;
	uint32_t& iodepth;

	increment_stats_t  increment_stats;
	access_params_t    access_params;
	offset_released_t  offset_released;

	public: //---------------------------------------------------------------------
	Prwv2Engine(int fd_, uint32_t& iodepth_, increment_stats_t increment_stats_,
	            access_params_t access_params_, offset_released_t offset_released_)
	          : fd(fd_), iodepth(iodepth_), increment_stats(increment_stats_),
	            access_params(access_params_),
				offset_released(offset_released_)
	{
		DEBUG_MSG("constructor");

		threads.reset(new std::unique_ptr<std::thread>[max_iodepth]);
		for (int i = 0; i < max_iodepth; i++) {
			threads[i].reset(new std::thread( [this, i]{this->worker_thread(i);} ));
		}
	}

	~Prwv2Engine() {
		DEBUG_MSG("destructor");
		stop = true;
		for (int i = 0; i < max_iodepth; i++) {
			if (threads[i]->joinable())
				threads[i]->join();
		}
		threads.reset(nullptr);
	}

	bool is_multithread() {return true;}

	void make_requests(bool& stop_) {
		if (thread_exception) {
			stop = true;
			std::rethrow_exception(thread_exception);
		}

		if (stop != stop_)
			stop = stop_;
		if (wait_)
			wait_ = false;

		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}

	void wait() {
		wait_ = true;
	}

	void worker_thread(int pos) {
		try {
			size_t cur_size = -1;
			std::unique_ptr<aligned_buffer_t[]> buffer_mem;
			char* buffer = nullptr;
			bool write = false;

			while (!stop) {
				while (!stop && wait_) {
					std::this_thread::sleep_for(std::chrono::milliseconds(200));
				}
				if (stop) break;

				if (pos < iodepth) {
					auto params = access_params();

					assert(params.size > 0);
					if (cur_size != params.size) {
						DEBUG_MSG("(posix thread[{}]) request size changed from {} to {}", pos, cur_size, params.size);
						cur_size = params.size;
						buffer_mem.reset(new aligned_buffer_t[cur_size/sizeof(aligned_buffer_t)]);
						buffer = buffer_mem[0].data;
						randomizer.randomize_buffer(buffer, cur_size);
					} else if (params.write && write) { // randomize 5% of the buffer due to repeated writes
						randomizer.randomize_buffer(buffer, cur_size, 20);
					}

					write = params.write;

					iovec prw = { .iov_base = buffer, .iov_len = cur_size };
					ssize_t ret;

					if (params.write) {
						ret = pwritev2(fd, &prw, 1, params.offset, params.dsync ? RWF_DSYNC : 0);
					} else {
						ret = preadv(fd, &prw, 1, params.offset);
					}

					if (stop) break;

					offset_released(params.offset);

					if (ret > 0) {
						Stats st = {
							.blocks = 1,
							.blocks_read  = static_cast<uint64_t>( (!params.write) ? 1 : 0 ),
							.blocks_write = static_cast<uint64_t>( ( params.write) ? 1 : 0 ),
							.KB_read  = (!params.write) ? params.block_size : 0,
							.KB_write = ( params.write) ? params.block_size : 0,
						};
						//DEBUG_MSG("st: KB_read={}, KB_write={}", st.KB_read, st.KB_write);
						increment_stats(st);
					} else if (ret == 0) {
						spdlog::error("(posix thread[{}]) read/write returned zero", pos);
					} else {
						if (errno != EAGAIN && errno != EINTR)
							throw std::runtime_error(fmt::format("(posix thread[{}]) read/write error: {}",
									pos, alutils::strerror2(errno)).c_str());
					}
				} else {
					std::this_thread::sleep_for(std::chrono::milliseconds(500));
				}
			}
		} catch (std::exception &e) {
			DEBUG_MSG("(posix thread[{}]) exception received: {}", pos, e.what());
			thread_exception = std::current_exception();
		}
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Lock::"

class Lock {
	bool active = false;
	std::atomic_flag lock_flag = ATOMIC_FLAG_INIT;

	public: //---------------------------------------------------------------------

	Lock() {}
	Lock(bool active_) : active(active_) {}
	void activate() {active = true;}

	// https://en.cppreference.com/w/cpp/atomic/atomic_flag
	void lock() {
		if (!active) return;

		while (lock_flag.test_and_set(std::memory_order_acquire)) {  // acquire lock
#			if defined(__cpp_lib_atomic_flag_test)
			while (lock_flag.test(std::memory_order_relaxed))        // test lock
#			endif
			{
				std::this_thread::yield(); // yield
			}
		}
	}
	void unlock() {
		if (!active) return;
		lock_flag.clear(std::memory_order_release);
	}
};


////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "EngineController::"

class EngineController {
	Args*       args;
	int         filed = -1;

	std::thread        thread;
	std::exception_ptr thread_exception;
	bool               stop_ = false;

	public: //---------------------------------------------------------------------
	Stats stats;

	EngineController(Args* args_) : args(args_) {
		DEBUG_MSG("constructor");
		assert(args != nullptr);

		if (args->create_file)
			createFile();

		openFile();

		thread = std::thread( [this]{this->threadMain();} );
	}

	~EngineController() {
		DEBUG_MSG("destructor");
		stop_ = true;
		if (thread.joinable())
			thread.join();
		if (filed >= 0) {
			DEBUG_MSG("close file");
			close(filed);
			if (args->create_file && args->delete_file) {
				spdlog::info("delete file {}", args->filename);
				std::remove(args->filename.c_str());
			}
		}
	}

	bool isActive() {
		if (thread_exception) std::rethrow_exception(thread_exception);
		return !stop_;
	}

	void stop() noexcept {
		stop_ = true;
	}

	void wait(bool value=true) noexcept {
		args->wait = value;
	}

	private: //--------------------------------------------------------------------

	void createFile() {
		spdlog::info("creating file {}", args->filename);

		const uint32_t buffer_size = 1024 * 1024;
		alignas(aligned_buffer_size) char buffer[buffer_size];
		randomizer.randomize_buffer(buffer, buffer_size);

		auto fd = open(args->filename.c_str(), O_CREAT|O_RDWR|O_DIRECT, 0640);
		if (fd < 0)
			throw std::runtime_error(fmt::format("can't create file: {}:{}", fd, E2S(fd)).c_str());
		try {
			size_t write_ret;
			for (uint64_t i=0; i<args->filesize; i++) {
				if ((write_ret = write(fd, buffer, buffer_size)) == -1) {
					throw std::runtime_error(fmt::format("write error ({})", strerror(errno)));
				}
			}
			DEBUG_MSG("file created");
		} catch (std::exception& e) {
			close(fd);
			std::remove(args->filename.c_str());
			throw std::runtime_error(fmt::format("create file error: {}", e.what()));
		}
		close(fd);
	}

	void checkFile() {
		struct stat st;
		DEBUG_MSG("get file stats");
		if (stat(args->filename.c_str(), &st) == EOF)
			throw std::runtime_error("can't read file stats");
		if ((args->block_size * 1024) % st.st_blksize != 0)
			throw std::runtime_error("block size must be multiple of filesystem's block size");
		if (!args->create_file) {
			uint64_t size = st.st_size / 1024 / 1024;
			spdlog::info("File already created. Set --filesize={}.", size);
			if (size < 10)
				throw std::runtime_error("invalid --filesize");
			args->filesize = size;
		}
	}

	void openFile() {
		DEBUG_MSG("open file");

		checkFile();

		int flags = 0;
		std::string flags_str;
#		define useFlag(flagname) flags = flags|flagname; flags_str += fmt::format("{}{}", flags_str.length() == 0 ? "" : "|", #flagname)
		useFlag(O_RDWR);
		if (args->o_direct) {
			useFlag(O_DIRECT);
		} else if (args->io_engine == "libaio") {
			throw std::runtime_error("libaio engine only supports --o_direct=true (O_DIRECT)");
		}
		if (args->io_engine == "posix" && args->o_dsync) {
			useFlag(O_DSYNC);
		}
#		undef useFlag

		spdlog::info("opening file '{}' with flags {}", args->filename, flags_str);
		if (args->o_dsync && (args->io_engine == "libaio" || args->io_engine == "prwv2")) {
			spdlog::info("write requests will use flag RWF_DSYNC");
		}

		filed = open(args->filename.c_str(), flags, 0640);
		if (filed < 0) {
			throw std::runtime_error(fmt::format("can't open file: {}", filed, alutils::strerror2(errno)).c_str());
		}
	}

	const uint32_t  random_scale = 10000;

	Lock block_size_lock;
	typeof(Args::block_size) cur_block_size  = 0;
	uint64_t buffer_size = 0;
	uint64_t file_blocks = 0;
	uint64_t cur_block   = 0;

	std::unique_ptr<std::uniform_int_distribution<uint64_t>> rand_block;

	void check_arg_updates() {
		if (cur_block_size != args->block_size) { // check block size
			DEBUG_MSG("cur_block_size changed from {} to {}", cur_block_size, args->block_size);

			block_size_lock.lock();

			cur_block_size = args->block_size;
			buffer_size = cur_block_size * 1024; // KiB to B;
			file_blocks = (args->filesize * 1024) / cur_block_size;
			cur_block = file_blocks; // seek 0 if next sequential I/O

			rand_block.reset(new std::uniform_int_distribution<uint64_t>(0, file_blocks -1));

			block_size_lock.unlock();
		}
	}

	Lock                 increment_stats_lock;
	increment_stats_t    increment_stats_lambda  = nullptr;

	access_params_t      access_params_lambda    = nullptr;
	offset_released_t    offset_released_lambda  = nullptr;

	void init_lambdas() {
		DEBUG_MSG("initiating lambdas");
		check_arg_updates();

		//-----------------------------------------------------
		increment_stats_lambda = [this](const Stats& val)->void{
			increment_stats_lock.lock();
			stats += val;
			//DEBUG_MSG("main stats: KB_read={}, KB_write={}", stats.KB_read, stats.KB_write);
			increment_stats_lock.unlock();
		};

		//-----------------------------------------------------
		access_params_lambda = [this]()->AccessParams {
			AccessParams ret;

			ret.write = randomizer.randomize_ratio(args->write_ratio);

			block_size_lock.lock();

			ret.dsync      = args->o_dsync;
			ret.block_size = cur_block_size;
			ret.size       = buffer_size;

			if (randomizer.randomize_ratio(args->random_ratio)) { //random access
				cur_block = (*rand_block)(randomizer.rand_eng64);
			} else { //sequential access
				cur_block++;
				if (cur_block >= file_blocks) {
					cur_block = 0;
				}
			}
			ret.offset = cur_block * buffer_size;

			block_size_lock.unlock();

			return ret;
		};

		//-----------------------------------------------------
		offset_released_lambda = [this](long long offset)->void {};
		//-----------------------------------------------------
	}

	void threadMain() noexcept {
		spdlog::info("initiating worker thread");
		try {
			init_lambdas();

			uint64_t flush_count = 0;
			uint64_t last_writes = 0;

			std::unique_ptr<GenericEngine> engine;

			spdlog::info("using {} engine", args->io_engine);
			if (args->io_engine == "posix") {
				engine.reset(new PosixEngine(
				                      filed,
				                      increment_stats_lambda,
				                      access_params_lambda,
				                      offset_released_lambda));
			} else if (args->io_engine == "libaio") {
				engine.reset(new AIOEngine(
				                      filed,
				                      args->iodepth,
				                      increment_stats_lambda,
				                      access_params_lambda,
				                      offset_released_lambda));
			} else if (args->io_engine == "prwv2") {
				engine.reset(new Prwv2Engine(
				                      filed,
				                      args->iodepth,
				                      increment_stats_lambda,
				                      access_params_lambda,
				                      offset_released_lambda));
			} else {
				throw std::runtime_error("invalid or not implemented engine");
			}

			if (engine->is_multithread()) {
				increment_stats_lock.activate();
				block_size_lock.activate();
			}

			while (!stop_) {
				if (args->wait)
					spdlog::info("engine controller thread in wait mode");
				while (!stop_ && args->wait) {
					engine->wait();
					std::this_thread::sleep_for(std::chrono::milliseconds(200));
					if (! args->wait) {
						spdlog::info("exit wait mode");
						break;
					}
				}
				if (stop_) break;

				check_arg_updates();

				engine->make_requests(stop_);

				if (!stop_ && args->flush_blocks) {
					auto cur_blocks_write = stats.blocks_write;
					if ((cur_blocks_write - last_writes) >= args->flush_blocks) {
						fdatasync(filed);
					}
					last_writes = cur_blocks_write;
				}

			} // while (!stop_)

		} catch (std::exception &e) {
			DEBUG_MSG("exception received: {}", e.what());
			thread_exception = std::current_exception();
		}
		spdlog::info("engine controller thread finished");
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Reader::"

class Reader {
	Args* args;

	std::thread        thread;
	std::exception_ptr thread_exception;
	bool               stop_ = false;

	std::unique_ptr<alutils::Socket> socket_server;

	const int max_shift_report_time_tries = 2;
	std::atomic<int> shift_report_time_tries = 0;
	std::atomic<long int> shift_report_time_ms = 0;

	public: //---------------------------------------------------------------------
	Reader(Args* args_) : args(args_) {
		DEBUG_MSG("constructor");
		thread = std::thread( [this]{this->threadMain();} );
		if (args->socket != "") {
			spdlog::info("initiating command socket: {}", args->socket);
			auto handler_l = [this](alutils::Socket::HandlerData* data)->void{socket_handler(data);};
			alutils::Socket::Params p; p.buffer_size=4096;
			socket_server.reset(new alutils::Socket(
					alutils::Socket::tServer,
					args->socket.c_str(),
					handler_l,
					p
			));
		}
	}
	~Reader() {
		DEBUG_MSG("destructor");
		stop_ = true;
		socket_server.reset(nullptr);
		if (thread.joinable())
			thread.join();
	}

	bool isActive() {
		if (thread_exception) std::rethrow_exception(thread_exception);
		return !stop_;
	}

	void stop() noexcept {
		stop_ = true;
	}

	long int shiftReportTime_ms() {
		return shift_report_time_ms.exchange(0);
	}

	private: //--------------------------------------------------------------------
	void threadMain() noexcept {
		try {
			DEBUG_MSG("command reader thread initiated");
			const uint32_t buffer_size = 512;
			char buffer[buffer_size]; buffer[0] = '\0'; buffer[buffer_size -1] = '\0';

			std::string command;

			while (alutils::monitor_fgets(buffer, buffer_size -1, stdin, &stop_)) {
				for (char* c=buffer; *c != '\0'; c++)
					if (*c == '\n') *c = '\0';

				command = buffer;
				alutils::inplace_strip(command);
				handle_commands(command);
			}
			stop_ = true;
		} catch (std::exception& e) {
			DEBUG_MSG("exception received: {}", e.what());
			thread_exception = std::current_exception();
		}
		DEBUG_MSG("command reader thread finished");
	}

	void socket_handler(alutils::Socket::HandlerData* data) {
		if (stop_) return;
		OutputController oc([&, data](const string& msg){data->send(msg + "\n", false);});
		try {
			spdlog::info("command received from socket: {}", data->msg);
			oc.print_debug("message received: {}", data->msg);

			std::smatch sm;
			std::regex_search(data->msg, sm, std::regex("^(.*)"));

			if (sm.size() >= 2) {
				string command = sm.str(1);
				alutils::inplace_strip(command);
				handle_commands(command, oc);

			} else {
				oc.print_error("invalid command");
			}
		} catch (std::exception& e) {
			spdlog::error("socket exception: {}", e.what());
			oc.print_error("{}", e.what());
		}
	}

	void handle_commands(const std::string& command) {
		OutputController oc;
		handle_commands(command, oc);
	}

	void handle_commands(const std::string& command, OutputController& oc) {
		if (command == "stop") {
			oc.print_info("stop command received");
			stop_ = true;
		} else {
			try {
				std::smatch sm1;
				std::regex_search(command, sm1, std::regex("^(shift_report_time) (-?[0-9]+)"));
				if (sm1.size() >= 3) {
					auto aux = std::strtol(sm1.str(2).c_str(), nullptr, 10);

					uint32_t limit = 700*args->stats_interval;
					if (std::abs(aux) >= limit)
						throw std::runtime_error(fmt::format("Invalid shift time. Must be between -{} and {} ms.", limit, limit).c_str());

					long int expected = 0;
					if (shift_report_time_ms.compare_exchange_weak(expected, aux)) {
						shift_report_time_tries = 0;
						oc.print_info("set shift_report_time = {}ms", aux);
					} else if (shift_report_time_tries >= max_shift_report_time_tries) {
						shift_report_time_tries = 0;
						shift_report_time_ms = aux;
						oc.print_info("set shift_report_time = {}ms (overrided)", aux);
					} else {
						shift_report_time_tries++;
						oc.print_error("Failed to set shift_report_time = {}ms. The old value was applied yet. Try again later.", aux);
					}

				} else {
					args->executeCommand(command, oc);
				}
			} catch (std::exception& e) {
				oc.print_error("{}", e.what());
			}
		}
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Program::"

class Program {
	static Program* this_;
	std::unique_ptr<Args>   args;
	std::unique_ptr<EngineController> engine_controller;
	std::unique_ptr<Reader> reader;

	Clock execution_clock;

	std::atomic<bool> stop_ = false;

	std::thread report_thread;
	std::atomic<bool> report_thread_active = false;
	std::pair<bool, std::string> report_thread_exception {false, ""};

	public: //---------------------------------------------------------------------
	Program() {
		DEBUG_MSG("constructor");
		Program::this_ = this;
		std::signal(SIGTERM, Program::signalWrapper);
		std::signal(SIGINT,  Program::signalWrapper);
	}
	~Program() {
		DEBUG_MSG("destructor");
		std::signal(SIGTERM, SIG_DFL);
		std::signal(SIGINT,  SIG_DFL);
		Program::this_ = nullptr;
	}

	int main(int argc, char** argv) noexcept {
		using namespace std::chrono;

		spdlog::info("Initializing program access_time3 version {}", ROCKSDB_TEST_VERSION);
		try {
			args.reset(new Args(argc, argv));

			engine_controller.reset(new EngineController(args.get()));
			reader.reset(new Reader(args.get()));
			report_thread = std::thread([this](){ reportThreadMain(); });

			Defer df1([this]{ resetAll(); });

			while (engine_controller->isActive() && reader->isActive()) {
				auto cur_sec = execution_clock.s();
				while (args->command_script.size() > 0 && args->command_script[0].time < cur_sec) {
					CommandLine c = args->command_script.front();
					args->command_script.pop_front();
					spdlog::info("command_script time={}, command: {}", c.time, c.command);
					if (c.command == "stop") {
						stop_ = true;
						break;
					} else {
						args->executeCommand(c.command);
					}
				}
				if (stop_) break;

				if (args->duration > 0 && execution_clock.s() > args->duration) {
					spdlog::info("duration time exceeded: {} seconds", args->duration);
					break;
				}

				if (report_thread_exception.first) {
					throw std::runtime_error(report_thread_exception.second.c_str());
				}

				std::this_thread::sleep_for(milliseconds(500));
			}

		} catch (const std::exception& e) {
			spdlog::error(e.what());
			spdlog::info("exit(1)");
			return 1;
		}
		spdlog::info("exit(0)");
		return 0;
	}

	private: //--------------------------------------------------------------------

	void reportThreadMain() noexcept {
		report_thread_active = true;
		spdlog::info("report thread initiated");
		try {
			Clock correction_clock;
			uint64_t stats_interval_us = args->stats_interval * 1000000;
			uint64_t last_ms = 0;

			auto elapsed_stats = engine_controller->stats;
			args->changed = true;

			while (!stop_) {
				auto shift_us = reader->shiftReportTime_ms() * 1000;
				DEBUG_MSG("shift_us = {}", shift_us);
				uint64_t sleep_us = stats_interval_us - correction_clock.us() + shift_us;
				if (sleep_us >= (2*stats_interval_us))
					throw std::runtime_error(fmt::format("BUG: invalid sleep time in report thread: {}us", sleep_us).c_str());

				DEBUG_MSG("sleep_us = {}", sleep_us);
				std::this_thread::sleep_for(microseconds(sleep_us));
				if (stop_) break;
				correction_clock.reset();

				auto cur_ms = execution_clock.ms();
				auto cur_stats = engine_controller->stats;

				//DEBUG_MSG("cur_stats: KB_read={}, KB_write={}", cur_stats.KB_read, cur_stats.KB_write);
				if (! args->changed) {
					uint64_t elapsed_ms = cur_ms - last_ms;
					std::string aux_args = args->strStat();
					auto delta = cur_stats - elapsed_stats;

					//DEBUG_MSG("delta: KB_read={}, KB_write={}", delta.KB_read, delta.KB_write);
					std::string aux_str =
						fmt::format("\"time\":\"{}\"", execution_clock.s()) +
						fmt::format(", \"total_MiB/s\":\"{:.2f}\"", static_cast<double>((delta.KB_read + delta.KB_write) * 1000)/static_cast<double>(elapsed_ms * 1024) ) +
						fmt::format(", \"read_MiB/s\":\"{:.2f}\"",  static_cast<double>(delta.KB_read  * 1000)/static_cast<double>(elapsed_ms * 1024) ) +
						fmt::format(", \"write_MiB/s\":\"{:.2f}\"", static_cast<double>(delta.KB_write * 1000)/static_cast<double>(elapsed_ms * 1024) ) +
						fmt::format(", \"blocks/s\":\"{:.1f}\"",    static_cast<double>(delta.blocks   * 1000)/static_cast<double>(elapsed_ms) ) +
						fmt::format(", \"blocks_read/s\":\"{:.1f}\"",  static_cast<double>(delta.blocks_read  * 1000)/static_cast<double>(elapsed_ms) ) +
						fmt::format(", \"blocks_write/s\":\"{:.1f}\"", static_cast<double>(delta.blocks_write * 1000)/static_cast<double>(elapsed_ms) ) ;
					spdlog::info("STATS: {{{}, {}}}", aux_str, aux_args);

				} else { // args changed. skip stats for one period
					args->changed = false;
				}

				elapsed_stats = cur_stats;
				last_ms = cur_ms;
			}
		} catch (const std::exception& e) {
			report_thread_exception = {true, e.what()};
		}
		spdlog::info("report thread finished");
		report_thread_active = false;
	}

	void resetAll() noexcept {
		stop_ = true;
		if (reader.get() != nullptr)
			reader->stop();
		if (engine_controller.get() != nullptr)
			engine_controller->stop();
		if (report_thread.joinable()) {
			for (int i = 0; i < 20 && report_thread_active; i++) {
				std::this_thread::sleep_for(milliseconds(100));
			}
			if (report_thread_active)
				report_thread.detach();
			else
				report_thread.join();
		}
		engine_controller.reset(nullptr);
		reader.reset(nullptr);
	}

	static void signalWrapper(int signal) noexcept {
		if (Program::this_)
			Program::this_->signalHandler(signal);
	}
	void signalHandler(int signal) noexcept {
		spdlog::warn("received signal {}", signal);
		std::signal(signal, SIG_DFL);
		resetAll();
		std::raise(signal);
	}
};
Program* Program::this_;

////////////////////////////////////////////////////////////////////////////////////

int main (int argc, char** argv) {
	Program p;
	return p.main(argc, argv);
}
