#pragma once

#include <atomic>
#include <csignal>

inline std::atomic<bool> stop_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free, "Signal flag mora biti lock-free");
inline void stop_signal(int) { stop_requested.store(true, std::memory_order_relaxed); }
inline void install_signals()
{
    std::signal(SIGINT, stop_signal);
    std::signal(SIGTERM, stop_signal);
}
