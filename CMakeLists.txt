cmake_minimum_required(VERSION 3.14)
project(distributed_kvstore VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ──────────────────────────────────────────────
# Core library (all node components)
# ──────────────────────────────────────────────
set(KVSTORE_SOURCES
    src/common/murmurhash3.cpp
    src/storage/wal.cpp
    src/storage/storage_engine.cpp
    src/cluster/consistent_hash.cpp
    src/cluster/membership.cpp
    src/cluster/replication.cpp
    src/server/tcp_server.cpp
    src/server/coordinator.cpp
    src/client/kv_client.cpp
)

add_library(kvstore_lib STATIC ${KVSTORE_SOURCES})
target_include_directories(kvstore_lib PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kvstore_lib PUBLIC pthread)

# ──────────────────────────────────────────────
# Main node executable
# ──────────────────────────────────────────────
add_executable(kvstore_node src/main.cpp)
target_link_libraries(kvstore_node PRIVATE kvstore_lib)

# ──────────────────────────────────────────────
# CLI client tool
# ──────────────────────────────────────────────
add_executable(kv_cli tools/cli_client.cpp)
target_link_libraries(kv_cli PRIVATE kvstore_lib)

# ──────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
