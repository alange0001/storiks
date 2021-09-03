
BUILD_TYPE ?= Release

bin: bin-deps version_py
	$(info Executing target $@)
	@./storiksctl --version_h > src/version.h.new
	test -f "src/version.h" || cp -f src/version.h.new src/version.h
	test "$(shell md5sum src/version.h |cut -d' ' -f1 )x == $(shell md5sum src/version.h.new |cut -d' ' -f1 )x" || cp -f src/version.h.new src/version.h 
	+cd build && make

all: bin
	$(info Executing target $@)

clean:
	$(info Executing target $@)
	rm -f src/version.h
	rm -fr build

clean-3rd-party:
	$(info Executing target $@)
	rm -fr 3rd-party

BIN_DEPS += build
build: CMakeLists.txt
	$(info Executing target $@)
	mkdir build || true
	cd build && cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) .. && make clean

3rd-party/.dir_ok:
	$(info Executing target $@)
	mkdir 3rd-party || true
	touch $@

version_py:
	$(info Executing target $@)
	@./storiksctl --version_py > lib/storiks/version.py


#########################################################################################
OBJ_3rd_party_gflags = 3rd-party/gflags/build/lib/libgflags.a
BIN_DEPS += $(OBJ_3rd_party_gflags)
$(OBJ_3rd_party_gflags): 3rd-party/.dir_ok
	$(info Executing target $@)
	test -d 3rd-party/gflags || git clone -b "v2.2.2" --depth 1 -- https://github.com/gflags/gflags.git 3rd-party/gflags
	mkdir 3rd-party/gflags/build || true
	cd 3rd-party/gflags/build && cmake ..
	+cd 3rd-party/gflags/build && make

#########################################################################################
OBJ_3rd_party_fmt = 3rd-party/fmt/build/libfmt.a
BIN_DEPS += $(OBJ_3rd_party_fmt)
$(OBJ_3rd_party_fmt): 3rd-party/.dir_ok
	$(info Executing target $@)
	test -d 3rd-party/fmt || git clone -b "6.2.0" --depth 1 -- https://github.com/fmtlib/fmt.git 3rd-party/fmt
	mkdir 3rd-party/fmt/build || true
	cd 3rd-party/fmt/build && cmake ..
	+cd 3rd-party/fmt/build && make

#########################################################################################
OBJ_3rd_party_spdlog = 3rd-party/spdlog/build/libspdlog.a
BIN_DEPS += $(OBJ_3rd_party_spdlog)
$(OBJ_3rd_party_spdlog): 3rd-party/.dir_ok
	$(info Executing target $@)
	test -d 3rd-party/spdlog || git clone -b "v1.x" --depth 1 -- https://github.com/gabime/spdlog.git 3rd-party/spdlog
	mkdir 3rd-party/spdlog/build || true
	cd 3rd-party/spdlog/build && cmake ..
	+cd 3rd-party/spdlog/build && make

#########################################################################################
OBJ_3rd_party_alutils = 3rd-party/alutils/build/libalutils.a
BIN_DEPS += $(OBJ_3rd_party_alutils)
$(OBJ_3rd_party_alutils): 3rd-party/.dir_ok
	$(info Executing target $@)
	test -d 3rd-party/alutils || git clone -b "storiks-v0.1" --depth 1 -- https://github.com/alange0001/alutils.git 3rd-party/alutils
	+cd 3rd-party/alutils && make

#########################################################################################
OBJ_3rd_party_nlohmann = 3rd-party/nlohmann/nlohmann/json.hpp
BIN_DEPS += $(OBJ_3rd_party_nlohmann)
$(OBJ_3rd_party_nlohmann): 3rd-party/.dir_ok
	$(info Executing target $@)
	test -d 3rd-party/nlohmann/nlohmann || mkdir -p 3rd-party/nlohmann/nlohmann
	test -r $(OBJ_3rd_party_nlohmann) || wget "https://github.com/nlohmann/json/raw/v3.9.1/single_include/nlohmann/json.hpp" -O $(OBJ_3rd_party_nlohmann)

#########################################################################################
rocksdb: 3rd-party/rocksdb/db_bench
	$(info Executing target $@)

3rd-party/rocksdb/.dir_ok: 3rd-party/.dir_ok
	$(info Executing target $@)
	test -d 3rd-party/rocksdb || git clone -b "6.15.fb" --depth 1 -- https://github.com/facebook/rocksdb.git 3rd-party/rocksdb
	touch $@

ROCKSDB_DEPS += $(shell ls rocksdb_patches/*.h)
ROCKSDB_DEPS += 3rd-party/rocksdb/db/db_impl/db_impl_open.cc.orig
3rd-party/rocksdb/db/db_impl/db_impl_open.cc.orig: 3rd-party/rocksdb/.dir_ok rocksdb_patches/rcm.patch
	$(info Executing target $@)
	cd 3rd-party/rocksdb && patch -b -p 1 <../../rocksdb_patches/rcm.patch
	touch $@

ROCKSDB_DEPS += 3rd-party/rocksdb/include/.storiks_ok
3rd-party/rocksdb/include/.storiks_ok: 3rd-party/rocksdb/.dir_ok
	$(info Executing target $@)
	cd 3rd-party/rocksdb/include && test -d storiks || ln -fs ../../../rocksdb_patches/ storiks
	touch $@

ROCKSDB_DEPS += 3rd-party/rocksdb/include/.alutils_ok
3rd-party/rocksdb/include/.alutils_ok: 3rd-party/rocksdb/.dir_ok $(OBJ_3rd_party_alutils)
	$(info Executing target $@)
	cd 3rd-party/rocksdb/include && test -d alutils || ln -fs ../../alutils/include/alutils/
	touch $@

ROCKSDB_DEPS += 3rd-party/rocksdb/include/.nlohmann_ok
3rd-party/rocksdb/include/.nlohmann_ok: 3rd-party/rocksdb/.dir_ok $(OBJ_3rd_party_nlohmann)
	$(info Executing target $@)
	cd 3rd-party/rocksdb/include && test -d nlohmann || ln -fs ../../nlohmann/nlohmann/
	touch $@

ROCKSDB_EXTRA_LDFLAGS = "-l alutils -L $(shell pwd)/3rd-party/alutils/build -l procps -L $(shell pwd)/3rd-party/alutils/3rd-party/procps/proc/.libs"
3rd-party/rocksdb/db_bench: $(ROCKSDB_DEPS)
	$(info Executing target $@)
	+cd 3rd-party/rocksdb && PLATFORM_LDFLAGS=$(ROCKSDB_EXTRA_LDFLAGS) make DEBUG_LEVEL=0 shared_lib db_bench rocksdbjava

rocksdb-java:
	touch 3rd-party/rocksdb/db/db_impl/db_impl_open.cc
	+cd 3rd-party/rocksdb && PLATFORM_LDFLAGS=$(ROCKSDB_EXTRA_LDFLAGS) make DEBUG_LEVEL=0 rocksdbjava

rocksdb-install: rocksdb
	$(info Executing target $@)
ifndef ROCKSDB_PREFIX
	$(error ERROR: undefined ROCKSDB_PREFIX)
endif
	+cd 3rd-party/rocksdb && PLATFORM_LDFLAGS=$(ROCKSDB_EXTRA_LDFLAGS) make DEBUG_LEVEL=0 PREFIX=$(ROCKSDB_PREFIX) install-shared

#########################################################################################
YCSB: 3rd-party/YCSB/.dir_ok

3rd-party/YCSB/.dir_ok: 3rd-party/.dir_ok
	$(info Executing target $@)
	git clone --depth 1 https://github.com/brianfrankcooper/YCSB.git 3rd-party/YCSB
	cd 3rd-party/YCSB && mvn -pl :rocksdb-binding -am clean package
	wget -O 3rd-party/YCSB/core/target/htrace-core4-4.0.1-incubating.jar https://repo1.maven.org/maven2/org/apache/htrace/htrace-core4/4.0.1-incubating/htrace-core4-4.0.1-incubating.jar
	wget -O 3rd-party/YCSB/core/target/HdrHistogram-2.1.4.jar https://repo1.maven.org/maven2/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar
	wget -O 3rd-party/YCSB/core/target/jackson-core-asl-1.9.4.jar https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-asl/1.9.4/jackson-core-asl-1.9.4.jar
	wget -O 3rd-party/YCSB/core/target/jackson-mapper-asl-1.9.4.jar https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-mapper-asl/1.9.4/jackson-mapper-asl-1.9.4.jar
	touch $@

#########################################################################################
performancemonitor: 3rd-party/performancemonitor/.dir_ok

3rd-party/performancemonitor/.dir_ok: 3rd-party/.dir_ok
	$(info Executing target $@)
	test -d 3rd-party/performancemonitor || git clone --depth 1 -- https://github.com/alange0001/performancemonitor.git 3rd-party/performancemonitor
	touch $@

#########################################################################################
bin-deps: $(BIN_DEPS)
	$(info Executing target $@)
