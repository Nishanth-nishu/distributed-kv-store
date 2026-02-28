/// @file tcp_server.cpp
/// @brief TCP server implementation with SO_REUSEADDR, accept loop, and
///        per-connection dispatch to a thread pool.

#include "server/tcp_server.h"
#include "common/config.h"
#include "common/logger.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

namespace kvstore {

TCPServer::TCPServer(uint16_t port, int pool_size)
    : port_(port), pool_(pool_size) {}

TCPServer::~TCPServer() {
    Stop();
}

void TCPServer::SetHandler(RequestHandler handler) {
    handler_ = std::move(handler);
}

// ═══════════════════════════════════════════════════════
//  Lifecycle
// ═══════════════════════════════════════════════════════

void TCPServer::Start() {
    if (running_.exchange(true)) return;

    // Create socket
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0)
        throw std::runtime_error("socket() failed: " +
                                 std::string(std::strerror(errno)));

    // SO_REUSEADDR — allow quick restarts
    int opt = 1;
    ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw std::runtime_error("bind() failed on port " +
                                 std::to_string(port_) + ": " +
                                 std::strerror(errno));

    // Listen
    if (::listen(server_fd_, config::SOCKET_BACKLOG) < 0)
        throw std::runtime_error("listen() failed: " +
                                 std::string(std::strerror(errno)));

    LOG_INFO("TCP server listening on port ", port_);

    accept_thread_ = std::thread(&TCPServer::AcceptLoop, this);
}

void TCPServer::Stop() {
    if (!running_.exchange(false)) return;

    LOG_INFO("TCP server shutting down");

    // Close the listening socket to unblock accept()
    if (server_fd_ >= 0) {
        ::shutdown(server_fd_, SHUT_RDWR);
        ::close(server_fd_);
        server_fd_ = -1;
    }

    if (accept_thread_.joinable())
        accept_thread_.join();
}

bool TCPServer::IsRunning() const {
    return running_;
}

// ═══════════════════════════════════════════════════════
//  Accept loop
// ═══════════════════════════════════════════════════════

void TCPServer::AcceptLoop() {
    while (running_) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = ::accept(server_fd_,
                                 reinterpret_cast<sockaddr*>(&client_addr),
                                 &client_len);
        if (client_fd < 0) {
            if (!running_) break;  // Shutdown signal
            LOG_WARN("accept() failed: ", std::strerror(errno));
            continue;
        }

        // TCP_NODELAY for low-latency responses
        int one = 1;
        ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        // Dispatch to thread pool
        pool_.Submit([this, client_fd]() {
            HandleConnection(client_fd);
        });
    }
}

// ═══════════════════════════════════════════════════════
//  Connection handler — supports persistent connections
// ═══════════════════════════════════════════════════════

void TCPServer::HandleConnection(int client_fd) {
    // Keep the connection alive for multiple requests (pipelining)
    while (running_) {
        auto request = RecvMessage(client_fd);
        if (!request) break;  // Client disconnected or error

        if (handler_) {
            ByteBuffer response = handler_(*request);
            if (!SendMessage(client_fd, response)) break;
        } else {
            ByteBuffer err = MakeErrorResponse("No handler configured");
            if (!SendMessage(client_fd, err)) break;
        }
    }

    ::close(client_fd);
}

}  // namespace kvstore
