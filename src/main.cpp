/// @file main.cpp
/// @brief Entry point for a distributed KV store node.
///
/// Usage:
///   kvstore_node --node-id <id> --port <port> [--data-dir <dir>]
///                [--seed <host:port>] [--seed <host:port>] ...
///                [--N <replication>] [--R <read-quorum>] [--W <write-quorum>]
///                [--log-level debug|info|warn|error]

#include <iostream>
#include <string>
#include <vector>
#include <csignal>
#include <atomic>
#include <thread>
#include <sstream>

#include "common/config.h"
#include "common/logger.h"
#include "common/types.h"
#include "storage/storage_engine.h"
#include "cluster/consistent_hash.h"
#include "cluster/membership.h"
#include "server/coordinator.h"
#include "server/tcp_server.h"

using namespace kvstore;

// ── Global shutdown flag ──────────────────────────────
static std::atomic<bool> g_shutdown{false};

static void SignalHandler(int /*sig*/) {
    g_shutdown.store(true);
}

// ── Argument parsing ──────────────────────────────────
struct Args {
    std::string node_id   = "node1";
    uint16_t    port      = config::DEFAULT_PORT;
    std::string data_dir  = "/tmp/kvstore";
    int         N         = config::DEFAULT_REPLICATION_FACTOR;
    int         R         = config::DEFAULT_READ_QUORUM;
    int         W         = config::DEFAULT_WRITE_QUORUM;
    LogLevel    log_level = LogLevel::INFO;
    std::vector<std::pair<std::string, uint16_t>> seeds;
};

static void PrintUsage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " [OPTIONS]\n"
        << "\n"
        << "Options:\n"
        << "  --node-id <id>        Unique node identifier (default: node1)\n"
        << "  --port <port>         Listening port (default: 7000)\n"
        << "  --data-dir <dir>      Data directory (default: /tmp/kvstore)\n"
        << "  --seed <host:port>    Seed node address (repeatable)\n"
        << "  --N <int>             Replication factor (default: 3)\n"
        << "  --R <int>             Read quorum (default: 2)\n"
        << "  --W <int>             Write quorum (default: 2)\n"
        << "  --log-level <level>   debug|info|warn|error (default: info)\n"
        << "  --help                Show this message\n";
}

static Args ParseArgs(int argc, char* argv[]) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto NextVal = [&]() -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << arg << "\n";
                std::exit(1);
            }
            return argv[++i];
        };

        if (arg == "--node-id")   { args.node_id  = NextVal(); }
        else if (arg == "--port") { args.port = static_cast<uint16_t>(std::stoi(NextVal())); }
        else if (arg == "--data-dir") { args.data_dir = NextVal(); }
        else if (arg == "--N")    { args.N = std::stoi(NextVal()); }
        else if (arg == "--R")    { args.R = std::stoi(NextVal()); }
        else if (arg == "--W")    { args.W = std::stoi(NextVal()); }
        else if (arg == "--seed") {
            std::string val = NextVal();
            auto colon = val.rfind(':');
            if (colon == std::string::npos) {
                std::cerr << "Invalid seed format (expected host:port): " << val << "\n";
                std::exit(1);
            }
            std::string host = val.substr(0, colon);
            uint16_t p = static_cast<uint16_t>(std::stoi(val.substr(colon + 1)));
            args.seeds.emplace_back(host, p);
        }
        else if (arg == "--log-level") {
            std::string lvl = NextVal();
            if (lvl == "debug")      args.log_level = LogLevel::DEBUG;
            else if (lvl == "info")  args.log_level = LogLevel::INFO;
            else if (lvl == "warn")  args.log_level = LogLevel::WARN;
            else if (lvl == "error") args.log_level = LogLevel::ERROR;
        }
        else if (arg == "--help") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        else {
            std::cerr << "Unknown argument: " << arg << "\n";
            PrintUsage(argv[0]);
            std::exit(1);
        }
    }
    return args;
}

// ═══════════════════════════════════════════════════════
//  main
// ═══════════════════════════════════════════════════════

int main(int argc, char* argv[]) {
    Args args = ParseArgs(argc, argv);

    // ── Configure logger ──────────────────────────
    Logger::Instance().SetLevel(args.log_level);
    Logger::Instance().SetNodeId(args.node_id);

    LOG_INFO("========================================");
    LOG_INFO("  Distributed KV Store — Node Starting");
    LOG_INFO("========================================");
    LOG_INFO("  Node ID   : ", args.node_id);
    LOG_INFO("  Port      : ", args.port);
    LOG_INFO("  Data Dir  : ", args.data_dir);
    LOG_INFO("  Quorum    : N=", args.N, " R=", args.R, " W=", args.W);
    LOG_INFO("  Seeds     : ", args.seeds.size());
    LOG_INFO("========================================");

    // Validate quorum parameters
    if (args.R + args.W <= args.N) {
        LOG_WARN("R+W <= N: eventual consistency mode "
                 "(strong consistency requires R+W > N)");
    }

    // ── Install signal handlers ───────────────────
    std::signal(SIGINT,  SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    // ── Create components ─────────────────────────
    // 1. Storage engine (with WAL recovery)
    std::string node_data_dir = args.data_dir + "/" + args.node_id;
    StorageEngine storage(node_data_dir);
    storage.Recover();

    // 2. Consistent hash ring
    ConsistentHashRing ring(config::VIRTUAL_NODES_PER_NODE);
    ring.AddNode(args.node_id);

    // 3. Membership manager
    NodeInfo self;
    self.node_id = args.node_id;
    self.host    = "0.0.0.0";  // Will be resolved by peers
    self.port    = args.port;

    MembershipManager membership(self);

    // Wire membership changes to the hash ring
    membership.SetOnJoin([&ring](const NodeInfo& node) {
        ring.AddNode(node.node_id);
        LOG_INFO("Ring: added node '", node.node_id, "' — ring has ",
                 ring.NodeCount(), " nodes");
    });
    membership.SetOnLeave([&ring](const std::string& node_id) {
        ring.RemoveNode(node_id);
        LOG_WARN("Ring: removed node '", node_id, "' — ring has ",
                 ring.NodeCount(), " nodes");
    });

    // Add seed nodes
    for (const auto& [host, port] : args.seeds) {
        membership.AddSeed(host, port);
    }

    // 4. Coordinator
    Coordinator coordinator(args.node_id, storage, ring, membership,
                            args.N, args.R, args.W);

    // 5. TCP server
    TCPServer server(args.port, config::THREAD_POOL_SIZE);
    server.SetHandler([&coordinator](ByteBuffer& req) -> ByteBuffer {
        return coordinator.HandleRequest(req);
    });

    // ── Start everything ──────────────────────────
    try {
        server.Start();
        membership.Start();
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to start: ", e.what());
        return 1;
    }

    LOG_INFO("Node '", args.node_id, "' is ready on port ", args.port);

    // ── Wait for shutdown signal ──────────────────
    while (!g_shutdown.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG_INFO("Shutdown signal received — stopping gracefully");

    membership.Stop();
    server.Stop();

    LOG_INFO("Node '", args.node_id, "' stopped. Goodbye!");
    return 0;
}
