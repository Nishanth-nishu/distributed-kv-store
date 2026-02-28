#pragma once
/// @file tcp_server.h
/// @brief Multi-threaded TCP server using a thread pool.

#include <cstdint>
#include <functional>
#include <atomic>
#include <thread>

#include "common/protocol.h"
#include "common/thread_pool.h"

namespace kvstore {

/// Accept-loop TCP server that dispatches connections to a thread pool.
class TCPServer {
public:
    using RequestHandler = std::function<ByteBuffer(ByteBuffer&)>;

    /// @param port       Port to listen on.
    /// @param pool_size  Number of worker threads.
    TCPServer(uint16_t port, int pool_size);
    ~TCPServer();

    // Non-copyable
    TCPServer(const TCPServer&) = delete;
    TCPServer& operator=(const TCPServer&) = delete;

    void SetHandler(RequestHandler handler);

    void Start();
    void Stop();
    bool IsRunning() const;

private:
    void AcceptLoop();
    void HandleConnection(int client_fd);

    uint16_t           port_;
    int                server_fd_ = -1;
    RequestHandler     handler_;
    std::atomic<bool>  running_{false};
    std::thread        accept_thread_;
    ThreadPool         pool_;
};

}  // namespace kvstore
