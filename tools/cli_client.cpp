/// @file cli_client.cpp
/// @brief Interactive CLI client for the distributed KV store.
///
/// Usage:
///   kv_cli --host <host> --port <port>
///
/// Commands:
///   put <key> <value>   — Store a key-value pair
///   get <key>           — Retrieve a value
///   delete <key>        — Remove a key
///   info                — Show cluster information
///   help                — Show available commands
///   quit / exit         — Disconnect

#include <iostream>
#include <string>
#include <sstream>

#include "client/kv_client.h"
#include "common/types.h"

using namespace kvstore;

static void PrintHelp() {
    std::cout
        << "\n"
        << "  ╔═══════════════════════════════════════╗\n"
        << "  ║   Distributed KV Store — CLI Client   ║\n"
        << "  ╠═══════════════════════════════════════╣\n"
        << "  ║  put <key> <value>  Store a value     ║\n"
        << "  ║  get <key>          Retrieve a value  ║\n"
        << "  ║  delete <key>       Remove a key      ║\n"
        << "  ║  info               Cluster info      ║\n"
        << "  ║  help               Show this help    ║\n"
        << "  ║  quit               Exit              ║\n"
        << "  ╚═══════════════════════════════════════╝\n"
        << "\n";
}

static void HandlePut(KVClient& client, const std::string& key,
                       const std::string& value) {
    auto resp = client.Put(key, value);
    if (!resp) {
        std::cerr << "  ERROR: Connection lost\n";
        return;
    }
    auto status = static_cast<StatusCode>(resp->ReadUint8());
    if (status == StatusCode::OK) {
        std::cout << "  OK\n";
    } else {
        std::string err = resp->ReadString();
        std::cerr << "  ERROR: " << err << "\n";
    }
}

static void HandleGet(KVClient& client, const std::string& key) {
    auto resp = client.Get(key);
    if (!resp) {
        std::cerr << "  ERROR: Connection lost\n";
        return;
    }
    auto status = static_cast<StatusCode>(resp->ReadUint8());
    if (status == StatusCode::OK) {
        std::string value  = resp->ReadString();
        uint64_t timestamp = resp->ReadUint64();
        std::string origin = resp->ReadString();
        std::cout << "  VALUE: " << value << "\n"
                  << "  (timestamp=" << timestamp
                  << ", origin=" << origin << ")\n";
    } else if (status == StatusCode::NOT_FOUND) {
        std::cout << "  (not found)\n";
    } else {
        std::string err = resp->ReadString();
        std::cerr << "  ERROR: " << err << "\n";
    }
}

static void HandleDelete(KVClient& client, const std::string& key) {
    auto resp = client.Delete(key);
    if (!resp) {
        std::cerr << "  ERROR: Connection lost\n";
        return;
    }
    auto status = static_cast<StatusCode>(resp->ReadUint8());
    if (status == StatusCode::OK) {
        std::cout << "  OK (deleted)\n";
    } else {
        std::string err = resp->ReadString();
        std::cerr << "  ERROR: " << err << "\n";
    }
}

static void HandleInfo(KVClient& client) {
    auto resp = client.GetClusterInfo();
    if (!resp) {
        std::cerr << "  ERROR: Connection lost\n";
        return;
    }
    auto status = static_cast<StatusCode>(resp->ReadUint8());
    if (status != StatusCode::OK) {
        std::cerr << "  ERROR: Failed to get cluster info\n";
        return;
    }

    uint32_t count = resp->ReadUint32();
    std::cout << "\n  ── Cluster Members (" << count << ") ──\n";
    for (uint32_t i = 0; i < count; ++i) {
        std::string id   = resp->ReadString();
        std::string host = resp->ReadString();
        uint16_t port    = resp->ReadUint16();
        bool alive       = resp->ReadBool();
        std::cout << "    [" << (alive ? "ALIVE" : " DEAD") << "] "
                  << id << " (" << host << ":" << port << ")\n";
    }

    uint64_t store_size = resp->ReadUint64();
    std::cout << "  Local store size: " << store_size << " keys\n\n";
}

// ═══════════════════════════════════════════════════════
//  main
// ═══════════════════════════════════════════════════════

int main(int argc, char* argv[]) {
    std::string host = "localhost";
    uint16_t port = 7000;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) host = argv[++i];
        else if (arg == "--port" && i + 1 < argc)
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (arg == "--help") {
            std::cerr << "Usage: kv_cli --host <host> --port <port>\n";
            return 0;
        }
    }

    KVClient client(host, port);

    std::cout << "\n  Connecting to " << host << ":" << port << "...\n";
    if (!client.Connect()) {
        std::cerr << "  Failed to connect to " << host << ":" << port << "\n";
        return 1;
    }
    std::cout << "  Connected!\n";
    PrintHelp();

    std::string line;
    while (true) {
        std::cout << "kvstore> ";
        if (!std::getline(std::cin, line)) break;

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if (cmd.empty()) continue;

        if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "help") {
            PrintHelp();
        } else if (cmd == "put") {
            std::string key, value;
            iss >> key;
            std::getline(iss >> std::ws, value);
            if (key.empty() || value.empty()) {
                std::cerr << "  Usage: put <key> <value>\n";
                continue;
            }
            // Reconnect if needed
            if (!client.IsConnected() && !client.Connect()) {
                std::cerr << "  ERROR: Cannot reconnect\n";
                continue;
            }
            HandlePut(client, key, value);
        } else if (cmd == "get") {
            std::string key;
            iss >> key;
            if (key.empty()) {
                std::cerr << "  Usage: get <key>\n";
                continue;
            }
            if (!client.IsConnected() && !client.Connect()) {
                std::cerr << "  ERROR: Cannot reconnect\n";
                continue;
            }
            HandleGet(client, key);
        } else if (cmd == "delete") {
            std::string key;
            iss >> key;
            if (key.empty()) {
                std::cerr << "  Usage: delete <key>\n";
                continue;
            }
            if (!client.IsConnected() && !client.Connect()) {
                std::cerr << "  ERROR: Cannot reconnect\n";
                continue;
            }
            HandleDelete(client, key);
        } else if (cmd == "info") {
            if (!client.IsConnected() && !client.Connect()) {
                std::cerr << "  ERROR: Cannot reconnect\n";
                continue;
            }
            HandleInfo(client);
        } else {
            std::cerr << "  Unknown command: " << cmd
                      << " (type 'help' for commands)\n";
        }
    }

    std::cout << "  Goodbye!\n";
    return 0;
}
