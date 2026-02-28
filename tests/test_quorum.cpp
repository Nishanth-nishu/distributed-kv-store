/// @file test_quorum.cpp
/// @brief Unit tests for quorum logic and replication parameters.

#include <gtest/gtest.h>

#include "cluster/consistent_hash.h"
#include "common/config.h"

using namespace kvstore;

// ═══════════════════════════════════════════════════════
//  Quorum Parameter Validation Tests
// ═══════════════════════════════════════════════════════

TEST(QuorumTest, DefaultParamsGuaranteeStrongConsistency) {
    int N = config::DEFAULT_REPLICATION_FACTOR;
    int R = config::DEFAULT_READ_QUORUM;
    int W = config::DEFAULT_WRITE_QUORUM;

    // R + W > N ⟹ strong consistency (overlapping quorums)
    EXPECT_GT(R + W, N)
        << "Default quorum params must guarantee strong consistency";
}

TEST(QuorumTest, StrongConsistencyWith3Nodes) {
    // Classic DynamoDB/Cassandra config
    int N = 3, R = 2, W = 2;
    EXPECT_GT(R + W, N);

    // At least one node in read quorum has the latest write
    // Because: W + R = 4 > 3 = N
}

TEST(QuorumTest, EventualConsistencyConfig) {
    int N = 3, R = 1, W = 1;
    EXPECT_LE(R + W, N)
        << "R=1, W=1 should yield eventual consistency";
}

TEST(QuorumTest, WriteAllReadOneIsStrong) {
    int N = 3, R = 1, W = 3;
    EXPECT_GT(R + W, N)
        << "W=N, R=1 should still be strongly consistent";
}

TEST(QuorumTest, ReadAllWriteOneIsStrong) {
    int N = 3, R = 3, W = 1;
    EXPECT_GT(R + W, N);
}

// ═══════════════════════════════════════════════════════
//  Replication Node Selection Tests
// ═══════════════════════════════════════════════════════

TEST(QuorumTest, ReplicaNodesAreDistinct) {
    ConsistentHashRing ring(150);
    ring.AddNode("node1");
    ring.AddNode("node2");
    ring.AddNode("node3");

    for (int i = 0; i < 100; ++i) {
        std::string key = "test_key_" + std::to_string(i);
        auto nodes = ring.GetNodes(key, 3);
        ASSERT_EQ(nodes.size(), 3u);

        // All must be distinct
        std::set<std::string> unique(nodes.begin(), nodes.end());
        EXPECT_EQ(unique.size(), 3u)
            << "Key " << key << " got duplicate replica assignment";
    }
}

TEST(QuorumTest, ReplicaCountDegrades) {
    ConsistentHashRing ring(150);
    ring.AddNode("node1");

    // With only 1 node, requesting 3 replicas returns 1
    auto nodes = ring.GetNodes("key", 3);
    EXPECT_EQ(nodes.size(), 1u);
}

TEST(QuorumTest, AllKeysHavePrimary) {
    ConsistentHashRing ring(150);
    ring.AddNode("node1");
    ring.AddNode("node2");
    ring.AddNode("node3");

    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto nodes = ring.GetNodes(key, 3);
        ASSERT_FALSE(nodes.empty());
        EXPECT_EQ(nodes[0], ring.GetPrimaryNode(key));
    }
}

// ═══════════════════════════════════════════════════════
//  Failure Tolerance Tests
// ═══════════════════════════════════════════════════════

TEST(QuorumTest, SurviveOneNodeFailure) {
    // With N=3, W=2, R=2: cluster can tolerate 1 node failure
    int N = 3, W = 2, R = 2;
    int surviving = N - 1;  // 2 nodes alive

    EXPECT_GE(surviving, W) << "Cannot satisfy write quorum after 1 failure";
    EXPECT_GE(surviving, R) << "Cannot satisfy read quorum after 1 failure";
}

TEST(QuorumTest, CannotSurviveTwoNodeFailures) {
    int N = 3, W = 2, R = 2;
    int surviving = N - 2;  // 1 node alive

    EXPECT_LT(surviving, W) << "Should NOT satisfy write quorum after 2 failures";
    EXPECT_LT(surviving, R) << "Should NOT satisfy read quorum after 2 failures";
}
