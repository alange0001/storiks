cmake_minimum_required(VERSION 3.2)

project (rocksdb_test)

SET(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
SET(BUILD_SHARED_LIBS OFF)
#SET(CMAKE_EXE_LINKER_FLAGS "-static -static-libstdc++ -lrt -pthread -Wl,--whole-archive -lpthread -Wl,--no-whole-archive")
SET(CMAKE_EXE_LINKER_FLAGS "-lrt -pthread -lpthread")
list(APPEND THIRDPARTY_LIBS dl)

#################################################################

set(PROJECT_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(WORKSPACE "${CMAKE_CURRENT_SOURCE_DIR}/..")

# 3rd-party/gflags:
#   Source code: https://github.com/gflags/gflags.git
#   Download and compilation routines are in Makefile.
set(gflags_DIR "${PROJECT_DIR}/3rd-party/gflags")
add_definitions(-DGFLAGS=1)
include_directories("${gflags_DIR}/build/include")
list(APPEND THIRDPARTY_LIBS "${gflags_DIR}/build/lib/libgflags.a")

# 3rd-party/fmt:
#   Source code: https://github.com/fmtlib/fmt.git
set(FMT_DIR "${PROJECT_DIR}/3rd-party/fmt")
include_directories("${FMT_DIR}/include")
list(APPEND THIRDPARTY_LIBS "${FMT_DIR}/build/libfmt.a")

# 3rd-party/spdlog:
#   Source code: https://github.com/gabime/spdlog.git
set(SPDLOG_DIR "${PROJECT_DIR}/3rd-party/spdlog")
include_directories ("${SPDLOG_DIR}/include")
list(APPEND THIRDPARTY_LIBS "${SPDLOG_DIR}/build/libspdlog.a")

# 3rd-party/alutils:
#   Source code: https://github.com/alange0001/alutils.git
set(ALUTILS_DIR "${PROJECT_DIR}/3rd-party/alutils")
include_directories("${ALUTILS_DIR}/include")
list(APPEND THIRDPARTY_LIBS "${ALUTILS_DIR}/build/libalutils.a")
list(APPEND THIRDPARTY_LIBS ${ALUTILS_DIR}/3rd-party/procps/proc/.libs/libprocps.a)

# 3rd-party/nlohmann:
#   Source code: https://github.com/nlohmann/json.git
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rd-party/nlohmann")

#################################################################

add_executable(rocksdb_test rocksdb_test.cc util.cc args.cc)
target_link_libraries(rocksdb_test ${THIRDPARTY_LIBS})
set_property(TARGET rocksdb_test PROPERTY CXX_STANDARD 17)
#set_property(TARGET rocksdb_test PROPERTY POSITION_INDEPENDENT_CODE ON)

add_executable(access_time3 access_time3.cc access_time3_args.cc util.cc)
target_link_libraries(access_time3 ${THIRDPARTY_LIBS} aio)
set_property(TARGET access_time3 PROPERTY CXX_STANDARD 17)
#set_property(TARGET access_time3 PROPERTY POSITION_INDEPENDENT_CODE ON)

add_executable(test test.cc util.cc)
target_link_libraries(test ${THIRDPARTY_LIBS} aio)
set_property(TARGET test PROPERTY CXX_STANDARD 17)
#set_property(TARGET test PROPERTY POSITION_INDEPENDENT_CODE ON)
