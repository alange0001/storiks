// Copyright (c) 2020-present, Adriano Lange.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// LICENSE.GPLv2 file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <exception>
#include <limits>
#include <string>
#include <memory>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#ifndef V2S
#	define V2S(val) std::to_string(val).c_str()
#endif

#ifndef DEBUG_MSG
#	define DEBUG_F spdlog::debug
#	define DEBUG_MSG(format, ...) DEBUG_F("[{}] " __CLASS__ "{}(): " format, __LINE__, __func__ , ##__VA_ARGS__)
#endif

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ "Bitmap::"

class Bitmap {
	const uint64_t min_size = 10;
	const uint64_t chunk_size = sizeof(uint64_t) * 8;
	uint64_t       chunk_size_last;
	const uint64_t max_memory = (1000*1000*1000)/8; // 1Gb / 8bits/B = 119MiB
	const uint64_t full_chunk = std::numeric_limits<uint64_t>::max();
	uint64_t       full_chunk_last;

	uint64_t size;
	uint64_t chunks;
	uint64_t used = 0;
	uint64_t used_threshold;
	uint64_t colisions = 0;
	std::unique_ptr<uint64_t[]> bitmap;

	public:
	Bitmap(uint64_t size_, uint64_t used_threshold_=0) {
		DEBUG_MSG("initiated");
		resize(size_, used_threshold_);
	}

	void resize(uint64_t size_, uint64_t used_threshold_=0) {
		size = size_;  DEBUG_MSG("size   = {}", size);
		if (size < min_size)
			throw std::runtime_error(fmt::format("invalid Bitmap size (must be >= {})", min_size).c_str());
		chunks = 1 + size / (chunk_size);  DEBUG_MSG("chunks = {}", chunks);

		chunk_size_last = full_chunk_last = 0;
		for (uint64_t i = (chunks - 1) * chunk_size; i < size; i++) {
			chunk_size_last++;
			full_chunk_last = full_chunk_last | ((uint64_t)1 << (chunk_size_last - 1));
		}; DEBUG_MSG("chunk_size_last = {:<2}, full_chunk_last={}", chunk_size_last, bitstring(full_chunk_last));

		if ((chunks * sizeof(uint64_t)) > max_memory)
			throw std::runtime_error((std::string("Bitmap is requiring ") +
					V2S((chunks * sizeof(uint64_t))/(1024*1024)) +
					"MiB (the maximum is " + V2S(max_memory/(1024*1024)) + "MiB)").c_str());
		spdlog::info("Bitmap using {}KiB", (chunks * sizeof(uint64_t))/1024);
		bitmap.reset(static_cast<uint64_t*>(std::aligned_alloc(sizeof(uint64_t), sizeof(uint64_t) * chunks)));

		if (used_threshold_ == 0) {
			used_threshold = size - (size / 10); // 90%
		} else {
			if (used_threshold_ >= min_size && used_threshold_ <= size)
				used_threshold = used_threshold_;
			else
				throw std::runtime_error(fmt::format("invalid used_threshold={} (must be >= {} and <= size={})", used_threshold_, min_size, size).c_str());
		}; DEBUG_MSG("used_threshold = {}", used_threshold);

		clear();
	}

	void clear() {
		spdlog::info("cleaning bitmap (used={}, colisions={})", used, colisions);
		used = 0;
		colisions = 0;
		memset(bitmap.get(), 0, chunks * sizeof(uint64_t));
	}

	void auto_clear() {
		if (used >= used_threshold)
			clear();
	}

	uint64_t next_unused(uint64_t val) {
		if (val >= size)
			throw std::runtime_error(fmt::format("bit position {} is out of range (0-{})", val, size-1).c_str());

		auto_clear();

		uint64_t add_colision = 0;
		while (true) {
			uint64_t chunk_idx = val / chunk_size;
			uint64_t chunk_bits = bitmap[chunk_idx];
			//DEBUG_MSG("val={:<8}, chunk_idx={:<8}, chunk_bits={}", val, chunk_idx, bitstring(chunk_bits));
			if (chunk_bits != full(chunk_idx)) { // there are unused bits
				uint64_t val_bit, val_in_chunk;
				uint64_t cur_chunk_size = chunk_size_by_idx(chunk_idx);
				for (val_bit = (val % chunk_size);; val_bit = (val_bit + 1) % cur_chunk_size) {
					val_in_chunk = (uint64_t)1 << val_bit;
					if (! (chunk_bits & val_in_chunk))
						break;
					add_colision = 1;
				}

				bitmap[chunk_idx] = chunk_bits | val_in_chunk;
				used++;
				colisions += add_colision;

				val = (chunk_idx * chunk_size) + val_bit;
				//DEBUG_MSG("return {:<25}, chunk_bits={}", val, bitstring(bitmap[chunk_idx]));
				if (val >= size)
					throw std::runtime_error(fmt::format("BUG: bitmap next value={} >= size={}", val, size).c_str());
				return val;

			} else { // chunk is full. try next
				val = ((chunk_idx + 1) % chunks) * chunk_size;
				add_colision = 1;
			}
		}
	};

	uint64_t full(uint64_t chunk_idx) {
		return (chunk_idx < chunks -1) ? full_chunk : full_chunk_last;
	}

	uint64_t chunk_size_by_idx(uint64_t chunk_idx) {
		return (chunk_idx < chunks -1) ? chunk_size : chunk_size_last;
	}

	template <typename T>
	std::string bitstring(const T val) {
		const T max = sizeof(val)*8;
		char ret[max + 1];
		ret[max] = '\0';
		for (int i = 0; i < max; i++) {
			ret[max - 1 - i] = (((T)1 << i) & val) ? '1' : '0';
		}
		return ret;
	}
};

////////////////////////////////////////////////////////////////////////////////////
#undef __CLASS__
#define __CLASS__ ""
