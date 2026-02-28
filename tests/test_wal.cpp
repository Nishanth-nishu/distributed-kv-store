/// @file test_wal.cpp
/// @brief Unit tests for the Write-Ahead Log.

#include <gtest/gtest.h>
#include <filesystem>

#include "storage/wal.h"

using namespace kvstore;
namespace fs = std::filesystem;

class WALTest : public ::testing::Test {
protected:
    std::string test_dir_ = "/tmp/kvstore_test_wal_" +
                            std::to_string(::getpid());

    void SetUp() override {
        fs::create_directories(test_dir_);
    }

    void TearDown() override {
        fs::remove_all(test_dir_);
    }

    std::string WALPath() const { return test_dir_ + "/test.wal"; }
};

TEST_F(WALTest, CreateNewFile) {
    WriteAheadLog wal(WALPath());
    EXPECT_TRUE(fs::exists(WALPath()));
    EXPECT_EQ(wal.FileSize(), 0u);
}

TEST_F(WALTest, AppendAndReplay) {
    {
        WriteAheadLog wal(WALPath());
        wal.Append(OpType::PUT, "key1", "value1", 1000);
        wal.Append(OpType::PUT, "key2", "value2", 2000);
        wal.Append(OpType::DELETE_OP, "key1", "", 3000);
    }

    // Reopen and replay
    WriteAheadLog wal(WALPath());
    auto entries = wal.Replay();

    ASSERT_EQ(entries.size(), 3u);

    EXPECT_EQ(entries[0].op, OpType::PUT);
    EXPECT_EQ(entries[0].key, "key1");
    EXPECT_EQ(entries[0].value, "value1");
    EXPECT_EQ(entries[0].timestamp, 1000u);

    EXPECT_EQ(entries[1].op, OpType::PUT);
    EXPECT_EQ(entries[1].key, "key2");
    EXPECT_EQ(entries[1].value, "value2");
    EXPECT_EQ(entries[1].timestamp, 2000u);

    EXPECT_EQ(entries[2].op, OpType::DELETE_OP);
    EXPECT_EQ(entries[2].key, "key1");
    EXPECT_EQ(entries[2].timestamp, 3000u);
}

TEST_F(WALTest, ReplayDetectsCorruption) {
    {
        WriteAheadLog wal(WALPath());
        wal.Append(OpType::PUT, "good_key", "good_value", 100);
    }

    // Corrupt the file by appending garbage
    {
        std::ofstream f(WALPath(), std::ios::app | std::ios::binary);
        char garbage[] = {0x01, 0x02, 0x03, 0x04, 0x05};
        f.write(garbage, sizeof(garbage));
    }

    WriteAheadLog wal(WALPath());
    auto entries = wal.Replay();

    // Should recover the good entry and stop at corruption
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].key, "good_key");
}

TEST_F(WALTest, Truncate) {
    WriteAheadLog wal(WALPath());
    wal.Append(OpType::PUT, "key", "val", 100);
    EXPECT_GT(wal.FileSize(), 0u);

    wal.Truncate();
    EXPECT_EQ(wal.FileSize(), 0u);

    auto entries = wal.Replay();
    EXPECT_TRUE(entries.empty());
}

TEST_F(WALTest, LargeValues) {
    WriteAheadLog wal(WALPath());

    std::string big_key(256, 'K');
    std::string big_val(1024 * 100, 'V');  // 100 KB

    wal.Append(OpType::PUT, big_key, big_val, 42);

    auto entries = wal.Replay();
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].key, big_key);
    EXPECT_EQ(entries[0].value, big_val);
}

TEST_F(WALTest, EmptyKeyAndValue) {
    WriteAheadLog wal(WALPath());
    wal.Append(OpType::PUT, "", "", 0);

    auto entries = wal.Replay();
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_TRUE(entries[0].key.empty());
    EXPECT_TRUE(entries[0].value.empty());
}

TEST_F(WALTest, ManyEntries) {
    const int N = 1000;
    {
        WriteAheadLog wal(WALPath());
        for (int i = 0; i < N; ++i) {
            wal.Append(OpType::PUT,
                       "key_" + std::to_string(i),
                       "val_" + std::to_string(i),
                       static_cast<uint64_t>(i));
        }
    }

    WriteAheadLog wal(WALPath());
    auto entries = wal.Replay();
    ASSERT_EQ(entries.size(), static_cast<size_t>(N));
    EXPECT_EQ(entries.back().key, "key_999");
}
