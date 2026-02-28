/// @file test_storage_engine.cpp
/// @brief Unit tests for the StorageEngine.

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <vector>
#include <atomic>

#include "storage/storage_engine.h"

using namespace kvstore;
namespace fs = std::filesystem;

class StorageEngineTest : public ::testing::Test {
protected:
    std::string test_dir_ = "/tmp/kvstore_test_se_" +
                            std::to_string(::getpid());

    void SetUp() override {
        fs::remove_all(test_dir_);
    }

    void TearDown() override {
        fs::remove_all(test_dir_);
    }
};

// ── Basic CRUD ───────────────────────────────────────

TEST_F(StorageEngineTest, PutAndGet) {
    StorageEngine engine(test_dir_);
    engine.Put("user:1001", R"({"name":"Nishanth"})", 100, "node1");

    auto val = engine.Get("user:1001");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->value, R"({"name":"Nishanth"})");
    EXPECT_EQ(val->timestamp, 100u);
    EXPECT_EQ(val->origin_node, "node1");
}

TEST_F(StorageEngineTest, GetNonExistent) {
    StorageEngine engine(test_dir_);
    auto val = engine.Get("missing");
    EXPECT_FALSE(val.has_value());
}

TEST_F(StorageEngineTest, DeleteExisting) {
    StorageEngine engine(test_dir_);
    engine.Put("key", "value", 100, "n1");
    EXPECT_TRUE(engine.Delete("key", 200));
    EXPECT_FALSE(engine.Get("key").has_value());
}

TEST_F(StorageEngineTest, DeleteNonExistent) {
    StorageEngine engine(test_dir_);
    EXPECT_FALSE(engine.Delete("nope", 100));
}

TEST_F(StorageEngineTest, OverwriteWithNewerTimestamp) {
    StorageEngine engine(test_dir_);
    engine.Put("key", "old", 100, "n1");
    engine.Put("key", "new", 200, "n1");

    auto val = engine.Get("key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->value, "new");
    EXPECT_EQ(val->timestamp, 200u);
}

TEST_F(StorageEngineTest, RejectStaleWrite) {
    StorageEngine engine(test_dir_);
    engine.Put("key", "new", 200, "n1");
    EXPECT_FALSE(engine.Put("key", "old", 100, "n1"));

    auto val = engine.Get("key");
    EXPECT_EQ(val->value, "new");
}

TEST_F(StorageEngineTest, RejectStaleDelete) {
    StorageEngine engine(test_dir_);
    engine.Put("key", "val", 200, "n1");
    EXPECT_FALSE(engine.Delete("key", 100));
    EXPECT_TRUE(engine.Get("key").has_value());
}

// ── Crash recovery ───────────────────────────────────

TEST_F(StorageEngineTest, RecoverAfterCrash) {
    {
        StorageEngine engine(test_dir_);
        engine.Put("key1", "val1", 100, "n1");
        engine.Put("key2", "val2", 200, "n1");
        engine.Delete("key1", 300);
        // Destructor — simulates crash
    }

    // New engine recovers from WAL
    StorageEngine engine(test_dir_);
    engine.Recover();

    EXPECT_FALSE(engine.Get("key1").has_value());  // Was deleted

    auto val2 = engine.Get("key2");
    ASSERT_TRUE(val2.has_value());
    EXPECT_EQ(val2->value, "val2");
}

// ── ConditionalPut ───────────────────────────────────

TEST_F(StorageEngineTest, ConditionalPutAcceptsNewer) {
    StorageEngine engine(test_dir_);
    engine.Put("key", "old", 100, "n1");

    VersionedValue vv{"new", 200, "n2"};
    EXPECT_TRUE(engine.ConditionalPut("key", vv));
    EXPECT_EQ(engine.Get("key")->value, "new");
}

TEST_F(StorageEngineTest, ConditionalPutRejectsStale) {
    StorageEngine engine(test_dir_);
    engine.Put("key", "new", 200, "n1");

    VersionedValue vv{"old", 100, "n2"};
    EXPECT_FALSE(engine.ConditionalPut("key", vv));
    EXPECT_EQ(engine.Get("key")->value, "new");
}

// ── Bulk operations ──────────────────────────────────

TEST_F(StorageEngineTest, BulkPutAndGetAll) {
    StorageEngine engine(test_dir_);

    std::vector<std::pair<std::string, VersionedValue>> batch;
    for (int i = 0; i < 100; ++i) {
        batch.push_back({"key_" + std::to_string(i),
                         {"val_" + std::to_string(i),
                          static_cast<uint64_t>(i), "n1"}});
    }
    engine.BulkPut(batch);
    EXPECT_EQ(engine.Size(), 100u);

    auto all = engine.GetAllData();
    EXPECT_EQ(all.size(), 100u);
}

// ── Thread safety ────────────────────────────────────

TEST_F(StorageEngineTest, ConcurrentReadWrite) {
    StorageEngine engine(test_dir_);
    const int num_threads = 8;
    const int ops_per_thread = 500;
    std::atomic<int> errors{0};

    auto writer = [&](int id) {
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = "key_" + std::to_string(id) + "_" + std::to_string(i);
            engine.Put(key, "value", static_cast<uint64_t>(i), "n1");
        }
    };

    auto reader = [&](int id) {
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = "key_" + std::to_string(id) + "_" + std::to_string(i);
            engine.Get(key);  // May or may not exist — just shouldn't crash
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(writer, i);
        threads.emplace_back(reader, i);
    }

    for (auto& t : threads) t.join();

    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(engine.Size(), static_cast<size_t>(num_threads * ops_per_thread));
}
