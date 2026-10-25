#include "net.h"

#include <algorithm>
#include <cerrno>
#include <stdexcept>
#include <utility>
#ifndef _WIN32
#include <fcntl.h>
#include <sys/select.h>
#endif

namespace {
int socket_error()
{
#ifdef _WIN32
    return WSAGetLastError();
#else
    return errno;
#endif
}

bool interrupted(int error)
{
#ifdef _WIN32
    return error == WSAEINTR;
#else
    return error == EINTR;
#endif
}

std::runtime_error net_error(const char* operation)
{
    return std::runtime_error(std::string(operation) + ": socket error " + std::to_string(socket_error()));
}

void send_timeout(const Socket& socket)
{
#ifdef _WIN32
    DWORD timeout = 1000;
#else
    timeval timeout{1, 0};
#endif
    if (setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO,
                   reinterpret_cast<const char*>(&timeout), sizeof(timeout)) != 0)
        throw net_error("SO_SNDTIMEO");
}
}

Network::Network()
{
#ifdef _WIN32
    WSADATA data;
    int result = WSAStartup(MAKEWORD(2, 2), &data);
    if (result != 0)
        throw std::runtime_error("WSAStartup: " + std::to_string(result));
#endif
}

Network::~Network()
{
#ifdef _WIN32
    WSACleanup();
#endif
}

Socket::~Socket()
{
    close();
}

Socket::Socket(Socket&& other) noexcept : fd(other.fd), stopped(other.stopped.load())
{
    other.fd = invalid_socket;
}

Socket& Socket::operator=(Socket&& other) noexcept
{
    if (this != &other) {
        close();
        fd = other.fd;
        stopped.store(other.stopped.load());
        other.fd = invalid_socket;
    }
    return *this;
}

void Socket::close()
{
    if (fd == invalid_socket)
        return;
#ifdef _WIN32
    closesocket(fd);
#else
    ::close(fd);
#endif
    fd = invalid_socket;
}

void Socket::shutdown_all() const
{
    stopped.store(true);
    if (fd == invalid_socket)
        return;
#ifdef _WIN32
    ::shutdown(fd, SD_BOTH);
#else
    ::shutdown(fd, SHUT_RDWR);
#endif
}

void Socket::shutdown_write() const
{
    if (fd == invalid_socket)
        return;
#ifdef _WIN32
    ::shutdown(fd, SD_SEND);
#else
    ::shutdown(fd, SHUT_WR);
#endif
}

Socket listen_local(int port)
{
    Socket listener(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (listener.get() == invalid_socket)
        throw net_error("socket");
#ifndef _WIN32
    int reuse = 1;
    if (setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0)
        throw net_error("SO_REUSEADDR");
#endif
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<unsigned short>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
        throw net_error("bind");
    if (::listen(listener.get(), 32) != 0)
        throw net_error("listen");
    return listener;
}

Socket connect_to(const std::string& host, int port, int timeout_ms)
{
    Socket socket(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (socket.get() == invalid_socket)
        throw net_error("socket");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<unsigned short>(port));
    if (inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1)
        throw std::invalid_argument("host mora biti IPv4 adresa");
#ifdef _WIN32
    u_long nonblocking = 1;
    if (ioctlsocket(socket.get(), FIONBIO, &nonblocking) != 0)
        throw net_error("FIONBIO");
#else
    int flags = fcntl(socket.get(), F_GETFL, 0);
    if (flags < 0 || fcntl(socket.get(), F_SETFL, flags | O_NONBLOCK) < 0)
        throw net_error("fcntl nonblocking");
#endif
    if (::connect(socket.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        int error = socket_error();
#ifdef _WIN32
        bool pending = error == WSAEWOULDBLOCK || error == WSAEINPROGRESS;
#else
        bool pending = error == EINPROGRESS || error == EWOULDBLOCK || error == EINTR;
#endif
        if (!pending)
            throw net_error("connect");
        fd_set write_set, error_set;
        FD_ZERO(&write_set);
        FD_ZERO(&error_set);
        FD_SET(socket.get(), &write_set);
        FD_SET(socket.get(), &error_set);
        timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
        int result = ::select(static_cast<int>(socket.get() + 1), nullptr, &write_set, &error_set, &timeout);
        if (result < 0)
            throw net_error("select connect");
        if (result == 0)
            throw std::runtime_error("Connect timeout");
        int connection_error = 0;
#ifdef _WIN32
        int size = sizeof(connection_error);
#else
        socklen_t size = sizeof(connection_error);
#endif
        if (getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&connection_error), &size) != 0)
            throw net_error("SO_ERROR");
        if (connection_error != 0)
            throw std::runtime_error("connect: socket error " + std::to_string(connection_error));
    }
#ifdef _WIN32
    nonblocking = 0;
    if (ioctlsocket(socket.get(), FIONBIO, &nonblocking) != 0)
        throw net_error("FIONBIO blocking");
#else
    if (fcntl(socket.get(), F_SETFL, flags) < 0)
        throw net_error("fcntl blocking");
#endif
    send_timeout(socket);
    return socket;
}

Socket accept_one(const Socket& listener)
{
    SocketHandle fd;
    do {
        fd = ::accept(listener.get(), nullptr, nullptr);
    } while (fd == invalid_socket && interrupted(socket_error()));
    if (fd == invalid_socket)
        throw net_error("accept");
    Socket socket(fd);
    send_timeout(socket);
    return socket;
}

int local_port(const Socket& socket)
{
    sockaddr_in address{};
#ifdef _WIN32
    int size = sizeof(address);
#else
    socklen_t size = sizeof(address);
#endif
    if (getsockname(socket.get(), reinterpret_cast<sockaddr*>(&address), &size) != 0)
        throw net_error("getsockname");
    return ntohs(address.sin_port);
}

bool wait_readable(const Socket& socket, int timeout_ms)
{
    fd_set set;
    FD_ZERO(&set);
    FD_SET(socket.get(), &set);
    timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    int result = ::select(static_cast<int>(socket.get() + 1), &set, nullptr, nullptr, &timeout);
    if (result < 0) {
        if (interrupted(socket_error()))
            return false;
        throw net_error("select");
    }
    return result > 0;
}

void send_all(const Socket& socket, const char* data, std::size_t size)
{
    std::size_t sent = 0;
    while (sent < size) {
        if (socket.is_shutdown())
            throw std::runtime_error("Socket je ugasen");
        int count = static_cast<int>(std::min<std::size_t>(size - sent, 1024 * 1024));
#ifdef _WIN32
        int n = ::send(socket.get(), data + sent, count, 0);
#else
        int n = static_cast<int>(::send(socket.get(), data + sent, count, MSG_NOSIGNAL));
#endif
        if (n < 0 && interrupted(socket_error()))
            continue;
        if (n <= 0)
            throw net_error("send");
        sent += static_cast<std::size_t>(n);
    }
}

bool receive_all(const Socket& socket, char* data, std::size_t size,
                 std::chrono::steady_clock::time_point deadline)
{
    std::size_t received = 0;
    while (received < size) {
        if (socket.is_shutdown())
            throw std::runtime_error("Socket je ugasen");
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
        if (left <= 0)
            throw std::runtime_error("Istekao rok za TCP poruku");
        if (!wait_readable(socket, static_cast<int>(std::min<long long>(left, 100))))
            continue;
        if (socket.is_shutdown())
            throw std::runtime_error("Socket je ugasen");
        int count = static_cast<int>(std::min<std::size_t>(size - received, 1024 * 1024));
        int n = static_cast<int>(::recv(socket.get(), data + received, count, 0));
        if (n < 0 && interrupted(socket_error()))
            continue;
        if (n < 0)
            throw net_error("recv");
        if (n == 0) {
            if (received == 0)
                return false;
            throw std::runtime_error("Prekinuta nepotpuna TCP poruka");
        }
        received += static_cast<std::size_t>(n);
    }
    return true;
}
