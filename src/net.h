#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <string>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
using SocketHandle = SOCKET;
constexpr SocketHandle invalid_socket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using SocketHandle = int;
constexpr SocketHandle invalid_socket = -1;
#endif

class Network {
public:
    Network();
    ~Network();
    Network(const Network&) = delete;
    Network& operator=(const Network&) = delete;
};

class Socket {
public:
    explicit Socket(SocketHandle fd = invalid_socket) : fd(fd) {}
    ~Socket();
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    SocketHandle get() const { return fd; }
    void shutdown_all() const;
    void shutdown_write() const;
    bool is_shutdown() const { return stopped.load(); }

private:
    void close();
    SocketHandle fd;
    mutable std::atomic<bool> stopped{false};
};

Socket listen_local(int port);
Socket connect_to(const std::string& host, int port, int timeout_ms = 1000);
Socket accept_one(const Socket& listener);
int local_port(const Socket& socket);
bool wait_readable(const Socket& socket, int timeout_ms);
void send_all(const Socket& socket, const char* data, std::size_t size);
bool receive_all(const Socket& socket, char* data, std::size_t size,
                 std::chrono::steady_clock::time_point deadline);
