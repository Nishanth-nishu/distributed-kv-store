cmake_minimum_required(VERSION 3.14)

include(FetchContent)

# ── Google Test ────────────────────────────────────────
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# ── Test targets ───────────────────────────────────────
function(add_kvtest TEST_NAME TEST_SRC)
    add_executable(${TEST_NAME} ${TEST_SRC})
    target_link_libraries(${TEST_NAME}
        PRIVATE kvstore_lib GTest::gtest_main)
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

add_kvtest(test_consistent_hash  test_consistent_hash.cpp)
add_kvtest(test_storage_engine   test_storage_engine.cpp)
add_kvtest(test_wal              test_wal.cpp)
add_kvtest(test_quorum           test_quorum.cpp)
