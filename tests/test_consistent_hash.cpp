/// @file test_consistent_hash.cpp
/// @brief Unit tests for the consistent hash ring.

#include <gtest/gtest.h>
#include <set>
#include <map>
#include <cmath>

#include "cluster/consistent_hash.h"

using namespace kvstore;

class ConsistentHashTest : public ::testing::Test {
protected:
    ConsistentHashRing ring_{150};
};

// ── Basic operations ─────────────────────────────────

TEST_F(ConsistentHashTest, EmptyRingThrows) {
    EXPECT_THROW(ring_.GetPrimaryNode("key1"), std::runtime_error);
}

TEST_F(ConsistentHashTest, AddSingleNode) {
    ring_.AddNode("node1");
    EXPECT_EQ(ring_.NodeCount(), 1u);
    EXPECT_EQ(ring_.RingSize(), 150u);
    EXPECT_EQ(ring_.GetPrimaryNode("any_key"), "node1");
}

TEST_F(ConsistentHashTest, AddMultipleNodes) {
    ring_.AddNode("node1");
    ring_.AddNode("node2");
    ring_.AddNode("node3");
    EXPECT_EQ(ring_.NodeCount(), 3u);
    EXPECT_EQ(ring_.RingSize(), 450u);
}

TEST_F(ConsistentHashTest, DuplicateAddIsIdempotent) {
    ring_.AddNode("node1");
    ring_.AddNode("node1");
    EXPECT_EQ(ring_.NodeCount(), 1u);
    EXPECT_EQ(ring_.RingSize(), 150u);
}

TEST_F(ConsistentHashTest, RemoveNode) {
    ring_.AddNode("node1");
    ring_.AddNode("node2");
    ring_.RemoveNode("node1");
    EXPECT_EQ(ring_.NodeCount(), 1u);
    EXPECT_EQ(ring_.GetPrimaryNode("any_key"), "node2");
}

TEST_F(ConsistentHashTest, HasNode) {
    ring_.AddNode("node1");
    EXPECT_TRUE(ring_.HasNode("node1"));
    EXPECT_FALSE(ring_.HasNode("node2"));
}

// ── Consistency property ─────────────────────────────

TEST_F(ConsistentHashTest, ConsistentMappingAfterAddNode) {
    ring_.AddNode("node1");
    ring_.AddNode("node2");

    // Record where 1000 keys map
    std::map<std::string, std::string> original;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(i);
        original[key] = ring_.GetPrimaryNode(key);
    }

    // Add a third node
    ring_.AddNode("node3");

    // Check that keys that DIDN'T move still map to the same node
    int moved = 0;
    for (const auto& [key, old_node] : original) {
        std::string new_node = ring_.GetPrimaryNode(key);
        if (new_node != old_node) ++moved;
    }

    // Expect roughly 1/3 of keys to move (tolerance: ±15%)
    double ratio = static_cast<double>(moved) / 1000.0;
    EXPECT_GT(ratio, 0.15) << "Too few keys moved: " << moved;
    EXPECT_LT(ratio, 0.5)  << "Too many keys moved: " << moved;
}

// ── GetNodes (replication) ───────────────────────────

TEST_F(ConsistentHashTest, GetNodesReturnDistinctPhysicalNodes) {
    ring_.AddNode("node1");
    ring_.AddNode("node2");
    ring_.AddNode("node3");

    auto nodes = ring_.GetNodes("test_key", 3);
    ASSERT_EQ(nodes.size(), 3u);

    std::set<std::string> unique(nodes.begin(), nodes.end());
    EXPECT_EQ(unique.size(), 3u) << "GetNodes returned duplicate physical nodes";
}

TEST_F(ConsistentHashTest, GetNodesClampedToAvailable) {
    ring_.AddNode("node1");
    ring_.AddNode("node2");

    auto nodes = ring_.GetNodes("key1", 5);
    EXPECT_EQ(nodes.size(), 2u);
}

// ── Distribution uniformity ──────────────────────────

TEST_F(ConsistentHashTest, KeyDistributionIsBalanced) {
    ring_.AddNode("node1");
    ring_.AddNode("node2");
    ring_.AddNode("node3");

    std::map<std::string, int> counts;
    const int total_keys = 10000;

    for (int i = 0; i < total_keys; ++i) {
        std::string key = "uniform_test_key_" + std::to_string(i);
        counts[ring_.GetPrimaryNode(key)]++;
    }

    double expected = total_keys / 3.0;
    for (const auto& [node, count] : counts) {
        double deviation = std::abs(count - expected) / expected;
        EXPECT_LT(deviation, 0.2)
            << "Node " << node << " has " << count
            << " keys (expected ~" << expected << ")";
    }
}

// ── Determinism ──────────────────────────────────────

TEST_F(ConsistentHashTest, HashIsDeterministic) {
    ring_.AddNode("node1");
    ring_.AddNode("node2");

    std::string result1 = ring_.GetPrimaryNode("hello");
    std::string result2 = ring_.GetPrimaryNode("hello");
    EXPECT_EQ(result1, result2);
}
